# Broadcast Service — Detailed Flow Documentation

---

## Overview

The Broadcast Service is a **stateless microservice** responsible for consuming pre-built dispatch batches from Kafka, sending them to Meta's WhatsApp Cloud API in windows of 80, and reporting results back to the Messaging Service after every window via HTTP callback.

It never owns campaign state — it is a pure executor. Credentials are embedded in the Kafka event itself so no external service calls are needed in the hot path.

```
Messaging Service  ──Kafka──►  Broadcast Service  ──HTTP──►  Meta WhatsApp API
                                      │
                                      └── HTTP Callback (every 80) ──►  Messaging Service
```

### Single identifier: `phoneNumberId`

`phoneNumberId` is the **only identifier** in the broadcast service. It drives everything:

| Role | Usage |
|---|---|
| Kafka message key | Partition affinity — all batches for same phone land on same partition |
| `PhoneQueue` key | One queue per phone number, one worker per queue |
| Semaphore key | Meta enforces 80 concurrent requests per phone number |
| Meta API path param | `POST /{phoneNumberId}/messages` |
| Callback field | Tells Messaging Service which phone number's window completed |

There is no `wabaAccountId` in the broadcast service. The Messaging Service owns that mapping internally.

---

## Why Windows of 80?

Meta's WhatsApp Cloud API enforces a limit of **80 concurrent requests per phone number**. Sending more simultaneously results in 429 rate-limit errors.

The window size is set to exactly 80 (`MAX_CONCURRENT_WHATSAPP_REQUESTS`). This gives:

- **Maximum throughput** — always keeping exactly 80 requests in-flight per phone number
- **Clean semaphore semantics** — window size = permit count, so acquiring all permits upfront is safe and never starves
- **Frequent status updates** — Messaging Service gets a callback every 80 messages, not every 1000
- **Smaller callback payloads** — 80 results per POST is easy to bulk-update in one DB query

For a single phone number with 1 lakh (100,000) recipients:
- 100,000 ÷ 80 = **1,250 windows**
- Each window fires 80 concurrent Meta requests and one bulk callback
- Multiple phone numbers process their windows fully independently and in parallel

---

## Phase 1 — Kafka Message Ingestion (`BroadcastMessageConsumer`)

**Entry point:** `BroadcastMessageConsumer.consume()`

### What happens

1. A Kafka message arrives on topic `whatsapp.messages.outbound`.
2. The message **key is `phoneNumberId`** — all batches for the same phone number land on the same partition.
3. The consumer thread **immediately deserializes** the raw JSON into a `BroadcastMessageEvent`.
4. The event contains:
   - `campaign_id` — which campaign this batch belongs to
   - `phone_number_id` — the sending phone number (queue key, semaphore key, Meta API path param)
   - `access_token` — Bearer token embedded by Messaging Service at dispatch time
   - `payloads[]` — up to 1000 recipients, each with a fully pre-built `request_payload`
5. Consumer calls `batchCoordinator.addBatch(event, acknowledgment)` and **returns immediately**.
6. Kafka offset is **NOT acknowledged yet** — deferred until all windows complete.

### Why are credentials in the Kafka event?

The Messaging Service already holds `phone_number_id` and `access_token` when it builds the dispatch batch. Embedding them means:

- **Zero external calls** in the broadcast service hot path
- **No credential cache** to manage, no cache stampede on startup
- **No single point of failure** from a credential service being down
- Credential rotation is handled by the Messaging Service — broadcast service is oblivious

### Error handling

- If JSON deserialization fails, the offset is **immediately acknowledged** to prevent infinite reprocessing of a poison-pill message.
- Errors are logged with `phoneNumberId`, partition, and offset for debugging.

### Key design decisions

- **Manual ack mode** (`MANUAL_IMMEDIATE`) — full control over offset commits.
- **Consumer thread is never blocked** — all heavy work handed off to `whatsappExecutor`.
- **Max poll records = 100** — controls Kafka backpressure.
- **6 concurrent consumer threads** — matches Kafka topic partition count.

---

## Phase 2 — Batch Queuing (`BatchCoordinatorService.addBatch()`)

**Entry point:** `BatchCoordinatorService.addBatch()`

### What happens

1. Event + acknowledgment handle wrapped into a `QueuedBatch`.
2. A **per-phone `PhoneQueue`** is looked up (or created lazily) using `phoneNumberId` as the key.
3. `QueuedBatch` enqueued — O(1), non-blocking.
4. `AtomicBoolean` (`processing` flag) per queue ensures **exactly one worker** drains each phone's queue:
   - No worker running → `tryStartProcessing()` flips flag to `true`, submits `drainQueue()` to `whatsappExecutor`.
   - Worker already running → nothing submitted; existing worker picks up the new batch on its next loop iteration.

### Why per-phone queues?

- Different phone numbers are completely independent — they never block each other.
- All batches for the **same phone number** are processed **sequentially** to maintain the 80-concurrent-request contract.
- One worker per phone means the semaphore for that phone is never contended — only one thread ever holds permits at a time.

---

## Phase 3 — Queue Draining (`BatchCoordinatorService.drainQueue()`)

**Worker loop running on `whatsappExecutor` thread pool.**

### What happens

1. Worker polls its `PhoneQueue` in a loop.
2. For each batch polled → calls `processBatch()`, which blocks until all windows for that batch complete.
3. When `poll()` returns `null` (queue empty):
   - Calls `tryStopProcessing()` to atomically release the processing flag.
   - Re-checks `isEmpty()` to handle the race: a new batch may have arrived between `poll()` returning null and the flag being released.
   - Confirmed empty → worker exits cleanly.
   - New item found → re-acquires flag and continues.
4. On `shutdownRequested = true` → loop exits after the current window finishes.

### Race condition safety

`tryStartProcessing()` and `tryStopProcessing()` use `AtomicBoolean.compareAndSet()`. Exactly one worker is active per phone queue at all times, even when multiple Kafka consumer threads call `addBatch()` simultaneously for the same phone number.

---

## Phase 4 — Batch Processing (`BatchCoordinatorService.processBatch()`)

**Called once per Kafka message. Orchestrates all windows for that batch.**

### What happens

```
Kafka batch (up to 1000 recipients, one phoneNumberId)
    │
    ▼
partition into windows of 80
    │
    ├── Window 1 (80 recipients)
    │       ├── sendWindow()          → 80 concurrent Meta calls, wait for all 80
    │       └── reportWindowResults() → callback 80 results to Messaging Service
    │
    ├── Window 2 (80 recipients)
    │       ├── sendWindow()
    │       └── reportWindowResults()
    │
    └── Window N (remainder ≤ 80)
            ├── sendWindow()
            └── reportWindowResults()
                        │
                        ▼
              acknowledgment.acknowledge()
```

### Step by step

1. Extract `phoneNumberId` and `accessToken` from the event — no external call.
2. Partition recipient list into sublists of 80.
3. Loop over windows, checking `shutdownRequested` before each one.
4. For each window: `sendWindow()` blocks until all 80 Meta responses received, then `reportWindowResults()` fires immediately (non-blocking).
5. Acknowledge Kafka offset after ALL windows complete.
6. Update metrics.

### Why ack after all windows, not per window?

Kafka doesn't support partial message acknowledgment — the offset represents the whole message. If the pod crashes mid-batch, Kafka redelivers the whole batch. The Messaging Service deduplicates via `provider_message_id` (wamid) to handle duplicate sends safely.

### Error handling

If an exception escapes the window loop, the batch is still acknowledged to prevent infinite Kafka retry. The Messaging Service detects missing window callbacks and handles recovery.

---

## Phase 5 — Window Sending (`BatchCoordinatorService.sendWindow()`)

**Sends one window (≤ 80 recipients) to Meta fully concurrently.**

### Semaphore

```java
Semaphore semaphore = getSemaphoreForUser(userSemaphores, semaphoreLastUsed, phoneNumberId);
semaphore.acquire(window.size());  // acquire all permits upfront
```

- One `Semaphore` per `phoneNumberId`, stored in `userSemaphores` map.
- Permit count = 80 = `MAX_CONCURRENT_WHATSAPP_REQUESTS`.
- All permits acquired **upfront** — safe because:
  - Window size ≤ 80 = total permits → acquiring all never deadlocks
  - Exactly **one worker per phone number** — no contention on this semaphore ever
- Released in `finally` — no leaks on timeout or exception.

### Concurrent dispatch

```java
for (RecipientPayload recipient : window) {
    futures.add(CompletableFuture.supplyAsync(
        () -> callMeta(recipient, phoneNumberId, accessToken, campaignId),
        whatsappExecutor
    ));
}
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .get(60, TimeUnit.SECONDS);
```

- All 80 futures submitted at once, run concurrently on `whatsappExecutor`.
- Awaited with **60-second timeout** per window.
- After `allOf()` completes, all futures are already done — result collection never blocks.

### Timeout handling

If 60s expires: all futures cancelled, all recipients in the window marked failed with `"Window timeout after 60s"`, window loop continues to the next window.

---

## Phase 6 — Single Meta API Call (`BatchCoordinatorService.callMeta()`)

**Runs on `whatsappExecutor`. Called 80 times concurrently per window.**

1. Calls `WhatsappClient.sendMessage(requestPayload, phoneNumberId, accessToken)`.
2. `requestPayload` is the **complete pre-built JSON string** from the Kafka event — no construction at send time.
3. Success → extracts `wamid` from Meta's response messages array.
4. Failure → captures Meta's `error.code` and `error.message`.
5. Returns `RecipientResult` — **never throws**, always success or failure.

One failed call never cancels the other 79 futures — all exceptions are caught internally.

---

## Phase 7 — Window Result Callback (`BatchCoordinatorService.reportWindowResults()`)

**Called after every window. Fire-and-forget.**

### Request body (80 results per call)

```json
{
  "campaign_id": 123,
  "phone_number_id": "1234567890",
  "results": [
    {
      "recipient_id": 1,
      "message_id": 101,
      "contact_id": 201,
      "success": true,
      "provider_message_id": "wamid.abc123"
    },
    {
      "recipient_id": 2,
      "message_id": 102,
      "contact_id": 202,
      "success": false,
      "error_code": "131026",
      "error_message": "Message undeliverable"
    }
  ]
}
```

Uses WebFlux `WebClient.subscribe()` — never blocks the window loop.
POSTs to `POST /internal/broadcast/callbacks/message-results`.

### Messaging Service responsibility

On each 80-result callback:
- Single **bulk UPDATE** for all 80 recipients
- Updates statuses to `SENT` / `FAILED`
- Stores `wamid` for deduplication
- Increments campaign counters
- Checks if campaign is complete

### Callback frequency

| Old | New |
|---|---|
| 1 callback per 1000-recipient batch | 1 callback per 80-recipient window |
| Status visible after full batch | Status visible after first window (80 messages) |
| 1000 rows in one DB call | 80 rows per call (~12 calls per 1000-batch) |

---

## Phase 8 — Lifecycle & Cleanup

### Empty queue cleanup (`@Scheduled` every 5 minutes)

Removes `PhoneQueue` entries that are empty and not processing. Prevents unbounded map growth as phone numbers cycle through.

### Semaphore cleanup (`@Scheduled` every 1 hour)

Removes `Semaphore` entries for phone numbers inactive > 6 hours. Prevents memory growth.

### Graceful shutdown (`@PreDestroy`)

1. `shutdownRequested = true` — workers stop after current window finishes.
2. `whatsappExecutor.shutdown()` — no new tasks accepted.
3. `whatsappExecutor.awaitTermination(60s)` — in-flight Meta calls finish.
4. If 60s expires → `shutdownNow()`.
5. Final metrics logged.

**At-least-once on restart:** Unacknowledged batches redelivered by Kafka. Messaging Service deduplicates via wamid.

---

## Executor Architecture

| Executor | Purpose | Config |
|---|---|---|
| `broadcastExecutor` | Campaign-level orchestration | Fixed, size = `max-concurrent-users` (100) |
| `whatsappExecutor` | Meta API calls + drain loop workers | Core=500, Max=2000, Queue=10000 |
| `maintenanceExecutor` | Scheduled cleanup tasks | 2 daemon threads |

`CallerRunsPolicy` on `whatsappExecutor`: if queue fills up, the submitting thread runs the task itself — blocks the drain loop from starting the next window, applying natural backpressure to Kafka poll rate. Tasks are never dropped.

---

## Full Data Flow

```
Messaging Service
  │  Kafka: whatsapp.messages.outbound
  │  Key:   phone_number_id
  │  Value: { campaign_id, phone_number_id, access_token, payloads[1000] }
  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          BROADCAST SERVICE                               │
│                                                                          │
│  Kafka Consumer Threads (×6)                                             │
│  ┌──────────────────────┐                                                │
│  │ BroadcastMessage     │  deserialize → BroadcastMessageEvent           │
│  │ Consumer             │  addBatch(event, ack) → returns immediately    │
│  └──────────────────────┘                                                │
│              │                                                           │
│              ▼  keyed by phoneNumberId                                   │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ BatchCoordinatorService                                            │  │
│  │                                                                    │  │
│  │  PhoneQueue[phone_1] ──► one worker, sequential batches           │  │
│  │  PhoneQueue[phone_2] ──► one worker, sequential batches           │  │
│  │  PhoneQueue[phone_N] ──► one worker, sequential batches           │  │
│  │                                                                    │  │
│  │  processBatch() — partition 1000 recipients into windows of 80    │  │
│  │                                                                    │  │
│  │  ┌── Window 1 (80) ──────────────────────────────────────────┐    │  │
│  │  │  semaphore[phoneNumberId].acquire(80)                      │    │  │
│  │  │  submit 80 CompletableFutures → whatsappExecutor           │    │  │
│  │  │    callMeta(r1)  callMeta(r2)  ...  callMeta(r80)          │    │  │
│  │  │  allOf().get(60s)                                          │    │  │
│  │  │  semaphore[phoneNumberId].release(80)                      │    │  │
│  │  │  reportWindowResults(80) → fire-and-forget                 │    │  │
│  │  └───────────────────────────────────────────────────────────┘    │  │
│  │  ┌── Window 2 (80) ── same pattern ──────────────────────────┐    │  │
│  │  └───────────────────────────────────────────────────────────┘    │  │
│  │  ...                                                               │  │
│  │  ┌── Window N (≤80) ─────────────────────────────────────────┐    │  │
│  │  └───────────────────────────────────────────────────────────┘    │  │
│  │                                                                    │  │
│  │  acknowledgment.acknowledge() ← only after ALL windows done       │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
        │  ×80 concurrent per window           │  ×1 per window (async)
        ▼                                      ▼
  Meta WhatsApp API                  Messaging Service
  POST /{phoneNumberId}/messages     POST /internal/broadcast
                                          /callbacks/message-results
                                     (bulk UPDATE 80 rows per call)
```

---

## Configuration Reference

```yaml
campaign:
  max-concurrent-users: 100       # broadcastExecutor pool size
  semaphore-cleanup-enabled: true
  executor:
    core-pool-size: 500           # whatsappExecutor core threads
    max-pool-size: 2000           # whatsappExecutor max threads
    queue-capacity: 10000         # whatsappExecutor task queue

meta:
  api:
    base-url: https://graph.facebook.com/v23.0
    connect-timeout: 5000         # ms
    read-timeout: 30000           # ms

messaging:
  service:
    base-url: ${MESSAGING_SERVICE_URL:http://localhost:8080}

kafka:
  topics:
    outbound-messages: whatsapp.messages.outbound
  consumer:
    group-id: broadcast-service
    max-poll-records: 100
```

---

## Key Design Principles

1. **`phoneNumberId` is the single identifier** — drives queue, semaphore, Meta API call, and callback. No `wabaAccountId` in broadcast service.
2. **Consumer thread is never blocked** — Kafka threads only deserialize and enqueue. All I/O on `whatsappExecutor`.
3. **Credentials in the event** — `phone_number_id` and `access_token` travel with the Kafka message. Broadcast service is fully stateless.
4. **Pre-built payloads** — `request_payload` constructed by Messaging Service during preparation. Broadcast service has zero business logic.
5. **Window = 80 = semaphore permits** — matches Meta's per-phone limit exactly. One worker per phone means semaphore is never contended.
6. **Callback per window** — 80-result callbacks give faster status visibility and reliable, small HTTP payloads for bulk DB updates.
7. **Kafka ack after all windows** — at-least-once guarantee. Crash = Kafka redelivers. Messaging Service deduplicates via wamid.
8. **Graceful shutdown with awaitTermination** — in-flight Meta calls complete before JVM exits.
9. **Stateless service** — no DB, no campaign state. Recovery owned by Messaging Service.
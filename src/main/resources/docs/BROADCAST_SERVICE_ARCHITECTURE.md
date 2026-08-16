# Broadcast Service — Architecture

**Status:** proposal. Nothing implemented.
**Scope:** `com.aigreentick.services.broadcast`
**Companion:** `MESSAGING_SERVICE_CHANGES.md`

---

## 1. Purpose and boundary

The Broadcast Service is the **centralised send executor** for the platform. Every outbound
WhatsApp message that goes to Meta — campaign or conversation — passes through it, so that Meta's
per-phone-number rate limit has exactly one enforcement point.

It is deliberately **not** a campaign engine. It owns no campaign state, no templates, no
recipients, no scheduling.

| Owns | Does not own |
|---|---|
| Pacing sends to Meta's per-number rate | Campaign lifecycle, recipients, templates |
| Distributed rate accounting across instances | Message status, counters, retries-to-completion |
| Retry classification and circuit breaking | Credentials, WABA/phone-number mapping |
| Reporting per-recipient outcomes | Deciding *what* or *whether* to send |

**Design rule:** if a decision needs campaign context, it belongs in Messaging. If it needs Meta
context, it belongs here.

---

## 2. Key decisions

Three decisions shape everything else.

**2.1 Meta's limit is a rate, not a concurrency count.**
Verified: 80 mps default per business phone number, 1,000 by automatic upgrade, 20 fixed for
coexistence numbers, inclusive of inbound. The existing window-of-N-then-`allOf().get()` barrier
achieves `N / p99_latency`, not N per second. It is replaced by a token bucket and a continuous
dispatch loop.

**2.2 Rate is global; concurrency is local.**
Rate is metered in Redis, per `phoneNumberId`, shared by all instances. Concurrency is bounded
per-instance by the HTTP connection pool. A distributed semaphore is *not* used: it would add two
Redis round trips per message plus lease expiry and a reaper, and by Little's Law
(`in-flight = rate × latency`) concurrency only needs to be *sufficient* to sustain a rate that is
already correct.

**2.3 Capacity has an owner, and it is not this service.**
The WABA Service is the system of record for a number's tier; Messaging owns `configured_mps` /
`effective_mps` in `waba_throughput_state`. Broadcast **reads** capacity from Redis and **reports**
observed rate limits. It never calls Meta for metadata and never calls the WABA Service. One number,
one source of truth.

---

## 3. Flow

```
Messaging Service
   │  Kafka: whatsapp.messages.outbound        Kafka: whatsapp.capacity.updates
   │  key = campaignId (see §7.2)              key = phoneNumberId, compacted
   ▼                                                       │
┌──────────────────────────────────────────────────────────┼──────────────┐
│                    BROADCAST SERVICE (×N)                 ▼              │
│                                                   CapacityListener       │
│  DispatchListener ──► BatchIngestService                  │              │
│                            │                              ▼              │
│                            ▼                       Redis  wa:cap:{pnid}  │
│                    DispatchScheduler ◄──────────────────  wa:tb:{pnid}   │
│                     (per-number, fair                     wa:sent:{rid}  │
│                      across campaigns)                                   │
│                            │  acquire tokens                             │
│                            ▼                                             │
│                     SendExecutor ──► MetaClient ──► Meta Cloud API       │
│                            │              │                              │
│                            │              └─ 130429/131057 ──► CapacityDegrader
│                            ▼                                             │
│                     ResultCollector ──► ResultPublisher                  │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  Kafka: whatsapp.messages.results
                                ▼
                        Messaging Service
```

The `Messaging → Broadcast → Meta → Messaging` shape you have today is preserved. Two things change:
a token bucket sits before the Meta call, and results return over Kafka instead of a
fire-and-forget HTTP POST.

---

## 4. Components

### 4.1 `DispatchListener` (inbound)
Kafka consumer on `whatsapp.messages.outbound`, manual ack.
Deserialises, validates, hands to `BatchIngestService`, returns. Never blocks.
Deserialisation failure → **DLQ topic**, then ack. (Today: ack and silently discard up to 1,000
recipients.)

### 4.2 `BatchIngestService`
Registers the batch with the scheduler under `(phoneNumberId, campaignId)`.
Holds a **bounded** queue per phone number. When the queue exceeds its high-water mark, calls
`MessageListenerContainer.pause()`; resumes below the low-water mark. This is the backpressure path
that does not exist today — the current `ConcurrentLinkedQueue` is unbounded, so a slow Meta cannot
slow the consumer.

### 4.3 `CapacityListener` + `CapacityStore`
Consumes `whatsapp.capacity.updates` (log-compacted, key = `phoneNumberId`) and writes
`wa:cap:{phoneNumberId}` to Redis. Compaction means a restarting instance replays current capacity
for every number without a bulk API call.

`CapacityStore` is the read side, with a short-lived local cache (~1s) so the dispatch loop does not
read capacity on every chunk.

### 4.4 `RateLimiter` (Redis token bucket)
One Lua script, one round trip, atomic. Refills at `effectiveMps`, caps at `effectiveMps × burstSeconds`.

Two properties that matter:

- **Partial grants.** Ask for 100, get 37 → send 37 now, ask again. No barrier, no stall on the
  slowest request.
- **Returns wait time.** When the grant is 0, Redis returns micros until the next token, so the
  caller sleeps precisely instead of spinning.

Because it is subtraction from one shared bucket, N instances cannot exceed the limit:

```
limit 500 mps → instance 1 asks 300, gets 300
                instance 2 asks 300, gets 200
                total 500, never 600
```

No instance count is configured anywhere. Adding a third instance changes nothing.

### 4.5 `DispatchScheduler`
One virtual thread (or one pooled worker) per active phone number:

```
while work remains:
    granted, waitMicros = rateLimiter.acquire(pnid, chunkSize)
    if granted == 0: sleep(waitMicros); continue
    pick `granted` payloads by deficit round robin across campaigns on this number
    submit to SendExecutor — do NOT await
```

**Fairness:** deficit round robin across per-campaign sub-queues, deficit measured in granted
tokens. A 100k campaign cannot starve a 500-recipient one on the same number. Today's single FIFO
per phone makes this impossible.

### 4.6 `SendExecutor` + `MetaClient`
`POST /{phoneNumberId}/messages` with the pre-rendered payload.

- **Non-blocking** (`Mono`, no `.block()`), or virtual threads. 1,000 mps at 200 ms p99 needs ~200
  in-flight; that must not mean 200 platform threads.
- **`ConnectionProvider` configured explicitly.** Reactor Netty's default is
  `max(cores, 8) × 2` ≈ 16 on a 4-core pod — today this silently caps throughput below every other
  number in the system.
- Per-recipient isolation preserved: one failure never cancels its siblings.

### 4.7 `ErrorClassifier`
Shared with Messaging (`MetaErrorCatalog` promoted to the platform module — see companion doc).

| Class | Codes | Action |
|---|---|---|
| Rate limit | `4`, `80007`, `130429`, `131048` | Degrade number, short backoff, re-queue behind the meter |
| Pair rate limit | `131056` | Per-recipient only — do **not** degrade the number |
| Upgrade in progress | `131057` | `backoffUntil = now + 90s`; not a failure |
| Transient | `1`, `2`, `133016`, 5xx, timeouts | Retry, full-jitter exponential backoff, bounded attempts |
| Credential | `190`, `0`, `10` | Fail batch fast, signal Messaging to refresh the token |
| Permanent | everything else | Fail the recipient with the code |

**Retry storms are prevented structurally:** retries acquire tokens from the same bucket as first
attempts, so retrying cannot exceed the rate. This is stronger than any retry budget.

### 4.8 `CapacityDegrader`
On a rate-limit code: halve `effectiveMps` in `wa:cap`, set `backoffUntil`, and emit the observation
in the result stream so Messaging persists it durably.

`SET NX` on `wa:degradelock:{pnid}` (5s TTL) so a burst of 400 concurrent 429s produces **one**
degrade, not 400 halvings to 1 mps.

Redis is the fast path (milliseconds); MySQL via Messaging is the durable record. Both are needed.

### 4.9 `IdempotencyGuard`
Meta's `/messages` endpoint has no idempotency key, so duplicate suppression must happen before the
send:

```
SET wa:sent:{recipientId} "CLAIMED" NX PX <2 × processingTimeout>
  acquired     → send; on success store the wamid; on retryable failure DEL
  not acquired → skip, report as already-dispatched
```

Trade-off, stated openly: a crash between claim and send means that recipient is skipped on
redelivery and recovered later by Messaging's `ProcessingStuckCleanupJob`. That is a delayed
message. The alternative is a duplicate message to a customer, which is not recoverable. **This is a
product decision, not a technical default** — flag it before implementation.

### 4.10 `CircuitBreaker`
Resilience4j, instanced **per phoneNumberId**.
Trips on transport failures and 5xx **only** — never on 4xx business errors. A number whose
recipients are mostly invalid is a data problem, not an availability problem, and must not open a
breaker.

Kept separate from the rate limiter: the breaker answers *"is Meta reachable?"*, the bucket answers
*"am I allowed to send more?"*. Conflating them makes a rate limit look like an outage.

### 4.11 `ResultCollector` + `ResultPublisher`
Buffers per-recipient outcomes and publishes to `whatsapp.messages.results` (key = `campaignId`),
flushing on size (~200) or time (~2s), whichever first.

**Why Kafka instead of the current HTTP callback:** `MessagingCallbackClientImpl` uses `.subscribe()`
with an error lambda that only logs. One failed callback silently loses the status of an entire
window; rows sit in `PROCESSING` until a cleanup job resets them and Messaging re-sends messages Meta
already delivered. Making that HTTP path reliable means bounded retry plus a local durable outbox —
which is a worse Kafka. The broker is already a dependency; using it here is *less* code, not more.

*(Also note: the current HTTP client sends no `X-Internal-Api-Key` or `X-Internal-Caller`, while
`BroadcastCallbackController` requires both and rejects the caller otherwise. See companion doc.)*

---

## 5. Redis data model

```
wa:cap:{phoneNumberId}          HASH   TTL 24h, refreshed on write
    configuredMps, effectiveMps, tier, backoffUntilMs, updatedAtMs, source

wa:tb:{phoneNumberId}           HASH   TTL 1h idle
    tokens (fractional), lastRefillMicros

wa:sent:{recipientId}           STRING TTL 2 × processingTimeout
    "CLAIMED" → wamid

wa:degradelock:{phoneNumberId}  STRING TTL 5s, SET NX
```

Keyed on **Meta's `phoneNumberId` string**, not the platform `BIGINT` — the meter must key on what
Meta actually limits, and it is the only identifier Broadcast sees.

Single Redis with AOF is sufficient. Sentinel/Cluster only when Redis itself becomes the constraint;
the fallback in §6 covers the outage case without HA.

---

## 6. Fault tolerance

| Failure | Behaviour |
|---|---|
| **Redis unavailable** | Local bucket at `lastKnownEffectiveMps × 0.5 / assumedInstances`. Deliberately pessimistic — under-sending costs time, over-sending costs quality rating, which feeds back into the tier. Never fail open. `capacity.source = FALLBACK` gauge + alert. |
| **Capacity missing for a number** | Fall back to `default-mps` (80). **Never round up to 1,000.** Warn: Messaging has not published capacity for a number it is dispatching from. |
| **Meta 429 storm** | Degrade once (lock), backoff, drain behind the meter. |
| **Meta 5xx / timeout** | Breaker opens for that number only; other numbers unaffected. |
| **Instance crash mid-batch** | Offset unacked → Kafka redelivers → `IdempotencyGuard` suppresses already-sent recipients. |
| **Kafka broker down** | Consumer stalls (correct). `ResultPublisher` buffers to a bounded in-memory queue, then pauses the consumer — no silent loss. |
| **Poison message** | DLQ topic, then ack. Not ack-and-discard. |
| **Unexpected exception in a batch** | Report per-recipient failures **then** ack. Never ack a batch whose results were never reported. |
| **Graceful shutdown** | Stop accepting, drain in-flight (bounded wait), flush results, ack, exit. Drain workers must run on a **separate** executor from sends — today `@PreDestroy` shuts down the executor the drain workers are running on, which throws `RejectedExecutionException` and loses in-flight sends. |

---

## 7. Horizontal scaling

### 7.1 Statelessness
All shared state is in Redis or Kafka. No sticky routing, no leader, no instance count in config.
Scaling is `kubectl scale`.

### 7.2 Partitioning — the change that unlocks 1,000 mps
Today the topic is keyed on `phoneNumberId`, so one number → one partition → one consumer. A
1,000 mps number is pinned to a single pod and cannot be scaled out. This is currently the binding
constraint on ever reaching the high tier.

Once the Redis meter exists, **per-number single-consumer exclusivity is no longer needed for
correctness** — correctness comes from the meter. So key on `campaignId` and let batches spread
across instances. Broadcast recipients are mutually independent; there is no ordering requirement
between two recipients of the same campaign, so nothing is lost.

Recommended: 24 partitions (divisible by 2/3/4/6/8/12 for clean rebalancing).

### 7.3 Sizing

| Tier | Instances | Per-instance pool | In-flight @200ms | Redis ops/s (chunk 50) |
|---|---|---|---|---|
| 80 mps | 1 | 32 | 16 | ~2 |
| 500 mps | 1–2 | 64 | 100 | ~10 |
| 1,000 mps | 2–4 | 64–128 | 200 | ~20 |

Redis load is negligible because metering is per *chunk*, not per message.

### 7.4 Autoscaling
On **consumer lag**, not CPU. Broadcast is I/O-bound and rate-limited; CPU stays flat while lag
grows. Scale-out only helps when capacity exists — so cap replicas at
`ceil(Σ effectiveMps / perInstanceCapacity)`, or you add pods that queue for tokens they will never
get.

---

## 8. Observability

Micrometer → Prometheus. Today there is no Micrometer, no `/actuator/prometheus`, and no health
indicator — metrics are `AtomicLong` fields readable only through a `getStats()` that nothing
exposes. For a service whose whole job is rate-sensitive I/O, this is the largest operational gap
after the rate model itself.

**Metrics** (tagged `phone_number_id`; `campaign_id` only where cardinality allows):

| Metric | Type | Why |
|---|---|---|
| `broadcast.tokens.granted` / `.requested` | counter | ratio = pipeline starvation |
| `broadcast.capacity.effective_mps` / `.configured_mps` | gauge | divergence = degradation |
| `broadcast.capacity.source{source}` | gauge | **fallback must be alertable** |
| `broadcast.send.duration` | timer | feeds pool sizing via Little's Law |
| `broadcast.send.result{outcome,error_code}` | counter | 130429 rate is the key SLI |
| `broadcast.inflight` | gauge | vs pool size = saturation |
| `broadcast.queue.depth`, `broadcast.consumer.paused` | gauge | backpressure |
| `broadcast.circuit.state` | gauge | per number |
| `broadcast.results.publish.failures` | counter | status divergence |
| `broadcast.redis.latency` | timer | the meter is on the hot path |

**Alerts:**
- 130429 > 1% of sends for 5 min → meter wrong or capacity stale
- `capacity.source == FALLBACK` > 2 min → Redis or publisher broken
- `effective/configured < 0.5` for 15 min → number stuck degraded
- `granted/requested < 0.5` with rising queue depth → under-provisioned or over-claimed
- consumer lag rising while `inflight` flat → stall, not load

**Logging:** structured JSON, MDC carrying `campaignId`, `phoneNumberId`, `recipientId`,
`traceId` propagated from the Kafka header. INFO per batch, DEBUG per chunk, **never per message** —
1,000 mps of INFO lines will cost more than the sends.

**Health:** `/actuator/health` with readiness gated on Kafka and Redis; liveness on neither
(a Redis outage must not restart pods that are correctly running on fallback).

---

## 9. Project structure

Mirrors the Messaging Service's hexagonal layout, so both services read the same way.

```
com.aigreentick.services.broadcast
├── api/
│   └── internal/            health, stats, ops endpoints
├── application/
│   ├── port/in/             DispatchBatchUseCase, UpdateCapacityUseCase
│   ├── port/out/            RateLimiterPort, CapacityStorePort,
│   │                        MetaSendPort, ResultPublisherPort, IdempotencyPort
│   └── service/
│       ├── ingest/          BatchIngestService
│       ├── dispatch/        DispatchScheduler, FairnessSelector, SendExecutor
│       ├── capacity/        CapacityStore, CapacityDegrader
│       └── result/          ResultCollector
├── domain/
│   ├── model/               PhoneNumberCapacity, DispatchBatch, RecipientOutcome
│   └── policy/              ErrorClassifier, RetryPolicy, ThroughputTier
└── infrastructure/
    ├── kafka/               DispatchListener, CapacityListener, ResultPublisher, DLQ
    ├── redis/               RedisRateLimiter (+ Lua), RedisCapacityStore, RedisIdempotencyGuard
    ├── meta/                MetaWebClient, ConnectionProvider config
    ├── config/              properties, executors, resilience
    └── observability/       metrics, health indicators
```

**Delete on the way through:** `MetaApiClient`, `BroadcastConstants`, `BroadcastCounterAggregator`,
`BroadcastLifecycleProducer`, `BroadcastLifecycleConsumer` (all empty stubs), `WabaCredentialResolver`
(credentials arrive in the event), and the dead semaphore code in `ExecutorConfig`
(`getSemaphoreForUser`, `userSemaphores`, `semaphoreLastUsed`, `cleanupInactiveSemaphores`,
`getSemaphoreStats` — never called, yet documented in `flow.md` as load-bearing).

---

## 10. Configuration

```yaml
broadcast:
  dispatch:
    chunk-size: 50
    max-queued-batches-per-phone: 20
    queue-resume-threshold: 5
  rate-limit:
    burst-seconds: 1.5
    default-mps: 80              # never 1000
    fallback-fraction: 0.5
    max-sleep: 200ms
  meta:
    base-url: https://graph.facebook.com/v23.0
    connect-timeout: 3s
    read-timeout: 15s
    max-connections: 64          # sized from mps × p99 — no longer a silent default
  retry:
    max-attempts: 3
    base-backoff: 500ms
    max-backoff: 30s
  circuit-breaker:
    failure-rate-threshold: 50
    wait-duration-in-open-state: 30s
```

Nothing about throughput is compiled in. A tier change reaches the dispatcher through Redis with no
deploy and no restart.

---

## 11. Migration

Each phase is independently shippable and safe to stop after.

| Phase | Work | Needs Redis? |
|---|---|---|
| **0 — stop the bleeding** | Configure `ConnectionProvider`; reconcile the `50` vs `80` vs docs drift; fix the shutdown bug; add auth headers to the callback client; delete dead semaphore code | No |
| **1 — close the control loop** | Report `errorCode` in results; Messaging degrades on campaign 429s; fix per-campaign vs per-number claim overshoot | No |
| **2 — the meter** | Capacity topic + Redis bucket + continuous dispatch loop, replacing windows | Yes |
| **3 — reliability** | Bounded queue + consumer pause; results over Kafka; retry classification; breaker; idempotency; DLQ | Yes |
| **4 — scale** | Micrometer + alerts; deficit round robin; re-key topic; non-blocking send path | Yes |

**Phase 1 is where to start.** It needs no new infrastructure and closes a control loop that is
currently open — today a 130429 on a campaign send is never reported to Messaging, so `effective_mps`
never degrades and the next poll walks into the same limit. On its own it should remove most rate-limit
errors from campaign traffic.

---

## 12. Open questions

1. Is Redis provisioned? Neither service depends on it today. Phase 0–1 stand alone regardless.
2. One repository or two? `MetaErrorCatalog` and the capacity contract want to be shared, not forked.
3. Does the WABA Service consume Meta's `phone_number_quality_update` webhook? If so,
   `THROUGHPUT_UPGRADE` gives near-instant capacity updates and the hourly sync becomes a safety net.
4. Observed p99 to `graph.facebook.com` from your region, and current instance count — these size the
   pools and chunks. Worth measuring rather than guessing.
5. Are there real 1,000 mps numbers in production now? Determines whether §7.2 re-keying is urgent.
6. **Duplicate vs delay** (§4.9) — which is worse for your customers?

# Messaging Service — Required Changes

**Status:** proposal. Nothing implemented.
**Scope:** `com.apargo.services.messaging`
**Companion:** `BROADCAST_SERVICE_ARCHITECTURE.md`

---

## Summary

The Messaging Service already contains most of the throughput model the new Broadcast Service needs:
`waba_throughput_state` with the `configured_mps` / `effective_mps` split, `ThroughputStateManager`
with AIMD degrade-and-recover, hourly tier sync from the WABA Service, and `MetaErrorCatalog`. The
schema comment even names the intended Redis key (`wa:tb:{waba_phone_number_id}`).

**This is good architecture and it should not be redesigned.** What follows is the minimum set of
changes to make it work with a centralised Broadcast Service.

Two of them are bugs in the current code, independent of the new design.

| # | Change | Type | Priority |
|---|---|---|---|
| 1 | Report rate-limit outcomes from campaign sends | Gap | **Critical** |
| 2 | Per-number, not per-campaign, claim budget | Bug | **Critical** |
| 3 | Publish capacity to a Kafka topic | New | High |
| 4 | Consume the results topic | New | High |
| 5 | Add the 20 mps coexistence tier | Bug | Medium |
| 6 | Extend the result contract | Contract | Medium |
| 7 | Promote `MetaErrorCatalog` to the shared module | Refactor | Low |
| 8 | Fix the stale `wabaAccountId == phoneNumberId` javadoc | Doc | Low |

Explicitly **out of scope:** the domain model, the hexagonal layering, `BroadcastCallbackService`'s
idempotency, the claim-before-publish ordering in `CampaignDispatchService`, and the asymmetric
`adoptConfiguredMps`. All correct as they stand.

---

## 1. Close the rate-limit feedback loop *(critical)*

**Problem.** `ThroughputStateManager.recordRateLimit(...)` is called from exactly one place:
`MessageDispatchService:136` — the conversation dispatcher. When Broadcast receives a `130429` on a
campaign send, **nothing ever tells Messaging.** `effective_mps` is never reduced, `backoff_until` is
never set, and the next poll claims the same oversized batch and walks into the same limit.

The AIMD control loop is fully built and has **no sensor on the path that generates the most
traffic.**

**Change.** In `BroadcastCallbackService.record(...)`, when a result carries a rate-limit code, call
`throughput.recordRateLimit(organizationId, wabaPhoneNumberId)`.

- Once per callback batch, not per result — a batch of 400 rate-limited results is one rate-limit
  event, not 400 halvings down to 1 mps.
- `MetaErrorCatalog.rateLimited(code)` already exists for the classification.
- `errorCode` arrives as a `String` in the DTO and is an `Integer` in the catalog — parse
  defensively; a malformed code must not fail the batch.
- Treat `131057` (throughput upgrade in progress, up to ~1 minute) as a short backoff, **not** a
  degrade. The number is being upgraded, not overloaded — halving `effective_mps` here punishes
  exactly the number that just got faster.

**Why it matters.** This alone makes the existing machinery work on the campaign path. It needs no
Redis, no new topics, and no infrastructure — and it should remove most 130429s from campaign
traffic on its own.

---

## 2. Per-number, not per-campaign, claim budget *(critical)*

**Problem.** `CampaignDispatchService.dispatchOnce` computes:

```java
int claimSize = properties.campaignClaimSize(state.effectiveMps());
```

once **per campaign**. Three RUNNING campaigns sharing one `wabaPhoneNumberId` each claim a full
interval's worth of a budget that is defined **per number** — a 3× overshoot. Multiply by the number
of Messaging instances polling concurrently and it compounds further.

**Change.** Group RUNNING campaigns by `wabaPhoneNumberId` before claiming. Compute one budget per
number and divide it across that number's campaigns — equal shares, or weighted if you have campaign
priority.

```
per-number budget = effectiveMps × pollInterval   (capped)
per-campaign share = budget / campaignsOnThisNumber
```

**Note.** Once Broadcast's Redis meter is live, this stops being a *correctness* issue — the meter
enforces the real limit at send time regardless of what was claimed. But it remains a **fairness**
issue: without it, whichever campaign is scanned first claims the whole budget and the others get
nothing that round. Worth fixing on its own merits, and it can ship before any Redis work.

---

## 3. Publish capacity to Kafka

**New:** topic `whatsapp.capacity.updates`, key = **Meta's `phoneNumberId` string**, log-compacted.

```json
{
  "phoneNumberId": "123456789012345",
  "wabaPhoneNumberId": 42,
  "organizationId": 7,
  "configuredMps": 1000,
  "effectiveMps": 500,
  "tier": "HIGH_THROUGHPUT",
  "backoffUntilMs": 0,
  "updatedAtMs": 1755264000000
}
```

Published whenever `waba_throughput_state` changes: `syncConfiguredThroughput()`,
`recoverDegradedNumbers()`, `recordRateLimit(...)`, and row creation in `stateFor(...)`.

**Two details that matter.**

*Key on Meta's ID, not the platform BIGINT.* Broadcast only ever sees Meta's identifier, and the meter
must key on what Meta actually limits. `CampaignDispatchService` already resolves this mapping in step
2 via `wabaDirectory.findPhoneNumberById(...)`; the same lookup feeds the publisher.

*Log compaction is the point.* A restarting Broadcast instance replays current capacity for every
number from the topic — no bulk API, no cold-start stampede, no capacity-service dependency in the
startup path.

**Reuse the existing outbox.** This is an event about state that has already committed, which is
exactly what `outbox` + the relay are for. Do not add a second publishing mechanism.

---

## 4. Consume the results topic

**New:** consumer on `whatsapp.messages.results` calling the existing
`BroadcastCallbackService.record(...)`.

The receiving logic barely changes: `record(...)` is already idempotent by wamid, already one
transaction per batch, and already goes through `MessageStatusApplier` with its rank guard. Only the
transport changes.

**Why replace the HTTP callback.** `MessagingCallbackClientImpl` calls `.subscribe()` with an error
lambda that only logs. One failed callback silently loses the status of an entire window — rows sit in
`PROCESSING` until `ProcessingStuckCleanupJob` resets them, and Messaging then re-sends messages Meta
already delivered. `BroadcastCallbackController`'s own javadoc names this exact failure. Making the
HTTP path reliable requires bounded retry plus a durable local outbox, which is a worse Kafka.

**Keep `BroadcastCallbackController` during migration.** Both paths converge on the same service
method, so they can run side by side. Retire the HTTP endpoint once the topic is proven.

### 4a. Authentication bug on the existing HTTP path

`BroadcastCallbackController.requireBroadcastCaller(...)` requires `X-Internal-Api-Key` and
`X-Internal-Caller`, and rejects the call otherwise. `MessagingCallbackClientImpl` sets **only**
`Content-Type` — no internal headers at all.

Either the callback is currently failing authentication on every call, or the key is being injected by
a filter not present in the uploaded source. **Verify this before anything else** — if it is the
former, no campaign result has ever been recorded via this path, and that would explain a great deal.

---

## 5. Add the coexistence tier

`ThroughputTier` maps `STANDARD → 80` and `HIGH_THROUGHPUT → 1000`. Meta also documents a **fixed
20 mps** for "coexistence" numbers — those in use with both the WhatsApp Business app and Cloud API.

Today such a number falls through to `defaultMps` (80) and will be rate-limited continuously, with
the AIMD loop fighting a ceiling it can never reach.

Add the mapping. **Keep the existing unknown-tier-returns-default behaviour** — that decision is
correct and its javadoc explains why better than I could.

Depends on the WABA Service exposing coexistence status; confirm before implementing.

---

## 6. Extend the result contract

`MessageResultCallbackRequest.MessageResultItem` currently carries:

```
recipientId, messageId, success, providerMessageId, errorCode, errorMessage
```

Add:

| Field | Why |
|---|---|
| `attempts` | how many times Broadcast tried before reporting — needed to reconcile with `messages.attempts` |
| `retryable` | Broadcast has already classified the error; re-deriving it in Messaging duplicates the catalog |
| `sentAtMs` | actual send time, distinct from callback-receipt time, for latency measurement |

**Also worth aligning:** Broadcast's outbound DTO sends `contactId` and `messageStatus`; Messaging's
inbound record declares neither. Jackson currently drops them silently. Either add them to the
contract or remove them from the sender — a field that exists on one side and is silently discarded on
the other will eventually be assumed to work.

---

## 7. Share `MetaErrorCatalog`

Both services must classify Meta error codes identically. If they drift, one retries what the other
gives up on, and the two disagree about what `effective_mps` should be.

Move to `com.apargo.platform.contract` (or an equivalent shared module) alongside the capacity event
schema. **Do not fork it** — the catalog is well-reasoned and its comments carry the rationale for
each code.

Blocked on the repository question: single repo or two?

---

## 8. Correct the stale javadoc

`BroadcastMessageConsumer` and `BroadcastMessageEvent` (Broadcast side) both state:

> `wabaAccountId` from messaging service = `phoneNumberId` in broadcast service context. This is the
> same identifier.

This is **false**. `CampaignDispatchService` publishes `campaign.getWabaAccountId()` (a platform
`BIGINT`) and `number.providerPhoneNumberId()` (Meta's string ID) as two separate fields on the event.
They identify different things.

Anyone who trusts this comment and uses `wabaAccountId` in the Graph API path produces 100% failures
against a number that looks correctly configured. Fix on both sides.

---

## Sequencing

| Phase | Changes | Needs Redis / new topics? |
|---|---|---|
| **1** | 1, 2, 4a, 5, 8 | No |
| **2** | 3 (capacity topic) | Topic only |
| **3** | 4, 6 (results topic + contract) | Topic only |
| **4** | 7 (shared module) | No |

**Phase 1 is self-contained and high value.** It fixes two real bugs, closes the control loop, and
requires no new infrastructure — worth shipping regardless of what happens to the Broadcast Service
redesign.

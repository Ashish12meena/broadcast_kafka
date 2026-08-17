# Test Profile — Meta Simulation

**Scope:** `com.aigreentick.services.broadcast.infrastructure.meta.simulator`
**Activate with:** `SPRING_PROFILES_ACTIVE=test`

---

## Summary

Under the `test` profile the Broadcast Service sends nothing to Meta. Every `DispatchEvent` that
arrives is processed as normal, each send is answered locally with a dummy response in Meta's own
envelope, and a status progression for that send is posted back to a configured URL after random
delays.

With the profile off, the production path is unchanged.

---

## How the profile is enforced

`MetaCloudApiClient` is annotated `@Profile("!test")` and `SimulatedMetaClient` `@Profile("test")`.
Exactly one implementation of `MetaSendPort` exists at any time.

This is stronger than a flag inside the send method. Under the test profile the real client is **not
registered as a bean at all**, so there is no object in the container holding a WebClient pointed at
graph.facebook.com. A misconfigured property cannot cause a real send.

---

## What is simulated and what is not

| Component | Under `test` |
|---|---|
| HTTP call to Meta | **Replaced** |
| Delivery status webhooks | **Simulated** |
| Kafka ingest, offsets, backpressure | Real |
| Redis token bucket / pacing | Real |
| Circuit breaker | Real |
| Idempotency guard | Real |
| Retry classification and backoff | Real |
| Result publishing | Real |

Only the network call is replaced. Dispatch batches still arrive from the Messaging Service on
`whatsapp.messages.outbound` exactly as in production.

---

## The dummy send response

`SimulatedMetaClient` builds the JSON Meta would have returned, deserializes it with the same
`MetaSendResponse` record the real client uses, and maps it with the same `MetaResponseMapper`:

```
build JSON  ->  MetaSendResponse  ->  MetaResponseMapper  ->  SendResponse
```

The production client's only difference is where the JSON comes from. The structure returned is
therefore identical by construction rather than by resemblance — nothing downstream can tell the two
apart.

Every send is accepted. There is no failure injection: Meta's error paths are already covered by
`MetaErrorCatalog` and its tests.

Message ids are shaped like a wamid but prefixed `wamid.SIM` so a simulated id can never be mistaken
for a real one in a shared database.

---

## Status callbacks

Each accepted send walks the progression Meta uses — `sent`, `delivered`, `read` — posted to
`broadcast.simulator.callback-url` as a `whatsapp_business_account` webhook.

**Delays are random and cumulative.** Each status waits its own draw from `[min-delay, max-delay]`
*after* the previous one. Two properties follow:

- Messages overtake each other, as they do in production. A fixed delay would deliver statuses in
  send order — the one ordering a receiver is guaranteed to handle — so the interleaving that real
  traffic produces would never be exercised.
- A single message's own statuses stay ordered. Independent per-status draws would let `read` arrive
  before `delivered`, which Meta never does.

---

## Configuration

Everything identifying a send comes from the `DispatchEvent` that produced it:

| Webhook field | Source |
|---|---|
| `entry[].id` | `DispatchEvent.wabaAccountId` |
| `metadata.phone_number_id` | `DispatchEvent.phoneNumberId` |
| `statuses[].id` | the wamid returned by the simulated send |
| `statuses[].recipient_id` | the `to` field of the forwarded request payload |

So there is nothing per-number in YAML. The only settings are the callback endpoint and the delay
band:

```yaml
broadcast:
  simulator:
    callback-url: ${SIM_CALLBACK_URL:}      # supply this
    min-delay: 1s
    max-delay: 8s
    # headers:                              # optional, if the receiver needs auth
    #   X-Internal-Api-Key: ...
```

Left blank, sends are still simulated, one warning is logged, and no statuses are posted.

`wabaAccountId` is carried on `MetaSendPort.send(...)` purely so the simulator can report against the
right account; `MetaCloudApiClient` ignores it, since Meta addresses a send by phone number id alone.

---

## Known limits

- **Callbacks are fire-and-forget.** No retry, no durability. A dropped simulated webhook is a
  missing test status, not lost customer data.
- **Delays are held in memory.** A restart loses every pending status.
- **`entry[].id` carries the platform account id**, not Meta's WABA id — the two are different
  values and Meta's is not available to this service (see `MESSAGING_SERVICE_CHANGES.md` item 8). If
  the receiving webhook routes on `entry[].id` rather than `metadata.phone_number_id`, it will need
  to accept the platform id, or Messaging must start publishing Meta's WABA id on the dispatch event.

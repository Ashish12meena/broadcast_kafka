-- Token bucket for one WhatsApp phone number.
--
-- Read, refill and take must happen as one atomic step. Performed as separate commands, two
-- instances would both read the same token count and both spend it, which is the over-sending this
-- whole mechanism exists to prevent.
--
-- Grants are partial by design. A request for 100 that finds 37 tokens returns 37 rather than zero,
-- so the caller sends those immediately instead of stalling until a full allocation is available.
--
-- KEYS[1] wa:tb:{phoneNumberId}    bucket state
-- KEYS[2] wa:cap:{phoneNumberId}   capacity, written by the capacity listener
--         The braces are a Redis Cluster hash tag, so both keys land on the same slot.
--
-- ARGV[1] nowMicros
-- ARGV[2] requested
-- ARGV[3] burstSeconds
-- ARGV[4] bucketTtlSeconds
--
-- Returns { granted, waitMicros }
--   granted = -1  capacity is unknown; the caller falls back to a local estimate
--   granted =  0  nothing available now; waitMicros says how long until one token exists

local bucketKey   = KEYS[1]
local capacityKey = KEYS[2]

local nowMicros     = tonumber(ARGV[1])
local requested     = tonumber(ARGV[2])
local burstSeconds  = tonumber(ARGV[3])
local bucketTtl     = tonumber(ARGV[4])

local capacity = redis.call('HMGET', capacityKey, 'effectiveMps', 'backoffUntilMs')
local mps      = tonumber(capacity[1])
local backoff  = tonumber(capacity[2]) or 0

-- No capacity published for this number. Say so rather than guessing: the caller knows what a safe
-- local default is and this script does not.
if mps == nil or mps <= 0 then
  return { -1, 0 }
end

-- The number is suppressed, either by a rate limit we recorded or by an upgrade in progress.
local nowMillis = nowMicros / 1000
if backoff > nowMillis then
  local waitMicros = math.ceil((backoff - nowMillis) * 1000)
  return { 0, waitMicros }
end

local burst = mps * burstSeconds

local state       = redis.call('HMGET', bucketKey, 'tokens', 'lastRefillMicros')
local tokens      = tonumber(state[1])
local lastRefill  = tonumber(state[2])

-- A bucket seen for the first time starts full. Starting empty would stall a number for a full
-- second at the beginning of every campaign for no reason.
if tokens == nil or lastRefill == nil then
  tokens = burst
  lastRefill = nowMicros
end

local elapsedMicros = nowMicros - lastRefill
if elapsedMicros < 0 then
  elapsedMicros = 0
end

tokens = math.min(burst, tokens + (elapsedMicros * mps / 1000000))

local granted = math.floor(math.min(tokens, requested))
if granted > 0 then
  tokens = tokens - granted
end

redis.call('HSET', bucketKey, 'tokens', tokens, 'lastRefillMicros', nowMicros)
redis.call('EXPIRE', bucketKey, bucketTtl)

-- Telling the caller exactly how long to wait removes the need to poll. Without it every idle
-- worker would wake on a fixed timer and ask again, which at a thousand numbers is a great deal of
-- pointless traffic.
local waitMicros = 0
if granted <= 0 then
  waitMicros = math.ceil((1 - tokens) * 1000000 / mps)
  if waitMicros < 0 then
    waitMicros = 0
  end
end

return { granted, waitMicros }

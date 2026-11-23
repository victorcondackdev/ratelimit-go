-- ratelimit-go gcra v1
--
-- GCRA for a batch of keys, all-or-nothing.
--
-- KEYS[i]      one key per request; the value is the TAT in microseconds.
-- ARGV[1]      "take", "peek" or "refund".
-- ARGV[3i-1]   emission interval of KEYS[i] in microseconds.
-- ARGV[3i]     burst of KEYS[i].
-- ARGV[3i+1]   cost of KEYS[i].
--
-- Reply: {all_allowed, now_us, then per key: allowed, remaining,
-- retry_after_us, reset_after_us}. Refund replies {1, now_us}.
--
-- Time is read from the Redis server, so every application instance sees
-- the same clock. Microseconds keep every value below 2^53, where Lua's
-- doubles are exact integers.

if redis.replicate_commands then
  -- Redis < 5 replicates scripts verbatim by default, which would replay a
  -- non-deterministic TIME on replicas. Effects replication is the default
  -- from 5.0 on, where this call is a no-op.
  redis.replicate_commands()
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local mode = ARGV[1]
local tats = {}
local changed = {}
local out = {1, now}
local all = true

local function stored(key)
  local tat = tats[key]
  if tat == nil then
    tat = tonumber(redis.call('GET', key) or '0') or 0
  end
  return tat
end

for i = 1, #KEYS do
  local key = KEYS[i]
  local base = 2 + (i - 1) * 3
  local iv = tonumber(ARGV[base])
  local burst = tonumber(ARGV[base + 1])
  local cost = tonumber(ARGV[base + 2])
  local tat = stored(key)
  if mode == 'refund' then
    tats[key] = tat - iv * cost
    changed[key] = true
  else
    if tat < now then tat = now end
    local tau = iv * burst
    local newtat = tat + iv * cost
    local allow_at = newtat - tau
    if now < allow_at then
      all = false
      local rem = math.floor((tau - (tat - now)) / iv)
      if rem < 0 then rem = 0 end
      out[#out + 1] = 0
      out[#out + 1] = rem
      out[#out + 1] = allow_at - now
      out[#out + 1] = tat - now
    else
      local rem = math.floor((tau - (newtat - now)) / iv)
      if rem < 0 then rem = 0 end
      out[#out + 1] = 1
      out[#out + 1] = rem
      out[#out + 1] = 0
      out[#out + 1] = newtat - now
      if cost > 0 then
        tats[key] = newtat
        changed[key] = true
      end
    end
  end
end

if mode == 'refund' then
  for key, tat in pairs(tats) do
    if tat > now then
      redis.call('SET', key, string.format('%.0f', tat), 'PX', math.ceil((tat - now) / 1000))
    else
      redis.call('DEL', key)
    end
  end
  return {1, now}
end

if not all then out[1] = 0 end
if mode == 'take' and all then
  for key, _ in pairs(changed) do
    local tat = tats[key]
    redis.call('SET', key, string.format('%.0f', tat), 'PX', math.ceil((tat - now) / 1000))
  end
end
return out

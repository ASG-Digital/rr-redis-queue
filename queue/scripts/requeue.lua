-- Atomically requeues the current delivery or dead-letters it when exhausted.
--
-- KEYS:
--   1 payload, 2 delayed, 3 reserved, 4 dead-letter,
--   5 priorities, 6 delivery counts, 7 delivery tokens, 8 ready
-- ARGV:
--   1 task ID, 2 delivery token, 3 serialized payload,
--   4 available-at timestamp (ms), 5 current timestamp (ms),
--   6 maximum retries, 7 dead-letter payload TTL (seconds)
-- RETURNS:
--   1 requeued, 0 stale or no longer reserved, -1 dead-lettered

local token = redis.call('HGET', KEYS[7], ARGV[1])
if not token or token ~= ARGV[2] or not redis.call('ZSCORE', KEYS[3], ARGV[1]) then
    return 0
end

redis.call('SET', KEYS[1], ARGV[3])
redis.call('ZREM', KEYS[3], ARGV[1])
local deliveries = tonumber(redis.call('HGET', KEYS[6], ARGV[1]) or '0')

if deliveries <= tonumber(ARGV[6]) then
    if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
        redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
    else
        local priority = redis.call('HGET', KEYS[5], ARGV[1]) or '10'
        redis.call('ZADD', KEYS[8], priority, ARGV[1])
    end
    return 1
end

redis.call('ZADD', KEYS[4], ARGV[5], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[7])
return -1

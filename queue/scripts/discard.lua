-- Atomically dead-letters the current delivery without retrying it.
--
-- KEYS:
--   1 payload, 2 ready, 3 reserved, 4 delayed, 5 dead-letter,
--   6 delivery tokens
-- ARGV:
--   1 task ID, 2 delivery token, 3 current timestamp (ms),
--   4 dead-letter payload TTL (seconds)
-- RETURNS:
--   1 discarded, 0 stale or no longer reserved

local token = redis.call('HGET', KEYS[6], ARGV[1])
if not token or token ~= ARGV[2] or not redis.call('ZSCORE', KEYS[3], ARGV[1]) then
    return 0
end

redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZADD', KEYS[5], ARGV[3], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1

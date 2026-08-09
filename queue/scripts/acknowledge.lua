-- Atomically acknowledges only the currently reserved delivery.
--
-- KEYS:
--   1 payload, 2 ready, 3 delayed, 4 reserved, 5 dead-letter,
--   6 delivery tokens, 7 priorities, 8 delivery counts
-- ARGV:
--   1 task ID, 2 delivery token
-- RETURNS:
--   1 acknowledged, 0 stale or no longer reserved

local token = redis.call('HGET', KEYS[6], ARGV[1])
if not token or token ~= ARGV[2] or not redis.call('ZSCORE', KEYS[4], ARGV[1]) then
    return 0
end

redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
return 1

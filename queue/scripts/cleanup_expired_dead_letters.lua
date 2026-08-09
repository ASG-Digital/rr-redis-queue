-- Removes queue metadata after a dead-letter task's retention period.
-- Payloads use the same TTL and expire independently as Redis string keys.
--
-- KEYS:
--   1 dead-letter, 2 priorities, 3 delivery counts, 4 delivery tokens
-- ARGV:
--   1 current timestamp (ms), 2 dead-letter retention period (seconds)
-- RETURNS:
--   Number of dead-letter entries cleaned up

local dead_before = tonumber(ARGV[1]) - (tonumber(ARGV[2]) * 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', dead_before, 'LIMIT', 0, 1000)

for _, id in ipairs(expired) do
    redis.call('ZREM', KEYS[1], id)
    redis.call('HDEL', KEYS[2], id)
    redis.call('HDEL', KEYS[3], id)
    redis.call('HDEL', KEYS[4], id)
end

return #expired

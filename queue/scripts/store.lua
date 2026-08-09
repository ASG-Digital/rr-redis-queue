-- Atomically stores a new task and makes it ready or delayed.
--
-- KEYS:
--   1 payload, 2 ready, 3 delayed, 4 reserved, 5 dead-letter,
--   6 priorities, 7 delivery counts, 8 delivery tokens
-- ARGV:
--   1 task ID, 2 serialized payload, 3 priority,
--   4 available-at timestamp (ms), 5 current timestamp (ms)
-- RETURNS:
--   1 stored, 0 already stored, -1 inconsistent/orphaned existing state

local existing_payload = redis.call('GET', KEYS[1])
local queued = redis.call('ZSCORE', KEYS[2], ARGV[1])
    or redis.call('ZSCORE', KEYS[3], ARGV[1])
    or redis.call('ZSCORE', KEYS[4], ARGV[1])
    or redis.call('ZSCORE', KEYS[5], ARGV[1])

if queued then
    if existing_payload == ARGV[2] then
        return 0
    end

    return -1
end

if existing_payload and existing_payload ~= ARGV[2] then
    return -1
end

redis.call('SET', KEYS[1], ARGV[2])
redis.call('HSET', KEYS[6], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[7], ARGV[1], 0)
redis.call('HSET', KEYS[8], ARGV[1], 0)

if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
    redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
else
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
end

return 1

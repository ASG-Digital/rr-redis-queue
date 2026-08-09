-- Atomically makes due/recoverable tasks available and reserves a batch.
--
-- KEYS:
--   1 ready, 2 delayed, 3 reserved, 4 dead-letter,
--   5 priorities, 6 delivery counts, 7 delivery tokens
-- ARGV:
--   1 current timestamp (ms), 2 lease expiry timestamp (ms),
--   3 maximum retries, 4 batch limit, 5 dead-letter payload TTL (seconds)
-- RETURNS:
--   Flat groups of task ID, serialized payload, delivery token, delivery count

local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, id in ipairs(expired) do
    local deliveries = tonumber(redis.call('HGET', KEYS[6], id) or '0')
    redis.call('ZREM', KEYS[3], id)
    redis.call('HINCRBY', KEYS[7], id, 1)

    if deliveries <= tonumber(ARGV[3]) then
        local priority = redis.call('HGET', KEYS[5], id) or '10'
        redis.call('ZADD', KEYS[1], priority, id)
    else
        redis.call('ZADD', KEYS[4], ARGV[1], id)
        redis.call('EXPIRE', id, ARGV[5])
    end
end

local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, 1000)
for _, id in ipairs(due) do
    redis.call('ZREM', KEYS[2], id)
    local priority = redis.call('HGET', KEYS[5], id) or '10'
    redis.call('ZADD', KEYS[1], priority, id)
end

local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[4]) - 1)
local reserved = {}
for _, id in ipairs(ids) do
    local payload = redis.call('GET', id)
    local priority = redis.call('HGET', KEYS[5], id)
    if not priority then
        priority = redis.call('ZSCORE', KEYS[1], id) or '10'
        redis.call('HSET', KEYS[5], id, priority)
    end
    redis.call('ZREM', KEYS[1], id)

    if payload then
        local delivery = redis.call('HINCRBY', KEYS[6], id, 1)
        local token = redis.call('HINCRBY', KEYS[7], id, 1)
        redis.call('ZADD', KEYS[3], ARGV[2], id)
        table.insert(reserved, id)
        table.insert(reserved, payload)
        table.insert(reserved, tostring(token))
        table.insert(reserved, tostring(delivery))
    else
        redis.call('HDEL', KEYS[5], id)
        redis.call('HDEL', KEYS[6], id)
        redis.call('HDEL', KEYS[7], id)
    end
end

return reserved

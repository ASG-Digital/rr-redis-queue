-- Atomically quarantines a task whose stored payload or metadata is invalid.
--
-- KEYS:
--   1 ready, 2 reserved, 3 delayed, 4 dead-letter,
--   5 priorities, 6 delivery counts, 7 delivery tokens
-- ARGV:
--   1 task ID/payload key, 2 current timestamp (ms),
--   3 dead-letter payload TTL (seconds)
-- RETURNS:
--   1 discarded

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[4], ARGV[2], ARGV[1])
redis.call('EXPIRE', ARGV[1], ARGV[3])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
return 1

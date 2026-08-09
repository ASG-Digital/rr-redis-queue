# rr-redis-queue

This is a plugin for RoadRunner adding support for a Redis-backed queue.
This plugin is still under development and should not be considered production ready.

The supported Redis version targeted by this plugin is version 6, as that is what Azure Cache for Redis runs and
is what we use this plugin for.

The code in this repository is released under a MIT license.

The driver keeps ready, delayed, reserved, and dead-letter states in Redis. A
task remains durable until `Ack`; reservations that outlive
`visibility_timeout` are recovered up to `max_retries`, after which they are
dead-lettered. Redis Pub/Sub is only a low-latency notification mechanism; the
periodic ticker always reads the durable queue.

The queue implementation is inspired by the code found here: https://github.com/roadrunner-server/google-pub-sub


## Config

```yaml
redisqueue:
    connections:
        conn1:
            addrs:
                - "${REDIS_HOST:-127.0.0.1}:${REDIS_PORT:-6379}"
            username: "${REDIS_USERNAME}"
            password: "${REDIS_PASSWORD}"
            db: "${REDIS_QUEUE_DB:-5}"
            tls:
                root_ca: ../path/to/cert.pem

# Queue
jobs:
    num_pollers: 10
    pipeline_size: 100000
    pool:
        num_workers: 4
        max_jobs: 0
        allocate_timeout: 60s
        destroy_timeout: 60s

    pipelines:
        test-1:
            driver: redisqueue
            config:
                connection: conn1
                prefix: "q1"
                channel: "q1channel"
                prefetch_limit: 10
                visibility_timeout: 720
                max_retries: 1
                retry_delay: 5
                dead_letter_ttl: 604800
                ticker_interval: 1s
    consume: [ "test-1" ]
```

`visibility_timeout` must be longer than the longest permitted worker
execution. `max_retries` is the number of additional infrastructure deliveries
after the initial delivery. `dead_letter_ttl` controls how long payloads remain
available for diagnosis after the delivery budget is exhausted.

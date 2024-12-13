# rr-redis-queue

This is a plugin for roadrunner adding support for a redis backed queue.
This plugin is still under development and should not be considered production ready.

The supported redis version targetted by this plugin is version 6 as that is what "Azure cache for Redis" runs which
is what we use this plugin for.

The code in this repository is released under a MIT license.

This queue implementation is inspired by the code found here: https://github.com/roadrunner-server/google-pub-sub


## Config

```
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
                visibility_timeout: 30
    consume: [ "test-1" ]
```
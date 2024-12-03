module redisqueue

go 1.23

toolchain go1.23.3

require (
	github.com/redis/go-redis/extra/redisotel/v9 v9.7.0
	github.com/redis/go-redis/v9 v9.7.0
	github.com/roadrunner-server/api/v4 v4.16.0
	github.com/roadrunner-server/endure/v2 v2.6.1
	github.com/roadrunner-server/errors v1.4.1
	github.com/roadrunner-server/events v1.0.1
	go.opentelemetry.io/contrib/propagators/jaeger v1.32.0
	go.opentelemetry.io/otel v1.32.0
	go.opentelemetry.io/otel/sdk v1.22.0
	go.opentelemetry.io/otel/trace v1.32.0
	go.uber.org/zap v1.27.0
	golang.org/x/sys v0.27.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.7.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)

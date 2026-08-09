package client

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestRedisClientUsesConfiguredRetryBackoffs(t *testing.T) {
	config := &Config{
		Addrs:           []string{"127.0.0.1:6379"},
		MinRetryBackoff: 125 * time.Millisecond,
		MaxRetryBackoff: 750 * time.Millisecond,
	}

	client, err := NewRedisClient(zap.NewNop(), config)
	if err != nil {
		t.Fatalf("create Redis client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	redisClient, valid := client.(*redis.Client)
	if !valid {
		t.Fatalf("expected a single-node Redis client, got %T", client)
	}
	options := redisClient.Options()
	if options.MinRetryBackoff != config.MinRetryBackoff {
		t.Fatalf("expected minimum retry backoff %s, got %s", config.MinRetryBackoff, options.MinRetryBackoff)
	}
	if options.MaxRetryBackoff != config.MaxRetryBackoff {
		t.Fatalf("expected maximum retry backoff %s, got %s", config.MaxRetryBackoff, options.MaxRetryBackoff)
	}
}

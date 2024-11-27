package lock

import (
	"github.com/redis/go-redis/v9"
	"redis/client"
	"time"
)

type LockHandler struct {
	config *Config
	client redis.UniversalClient
}

func NewLockHandler(config *Config) (*LockHandler, error) {
	if config.Client == nil {
		config.Client = &client.Config{}
		config.Client.InitDefaults()
	}
	if config.RetryInterval == 0 {
		config.RetryInterval = 50 * time.Millisecond
	}
	return &LockHandler{
		config: config,
	}, nil
}

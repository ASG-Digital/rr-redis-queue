package lock

import (
	"redis/client"
	"time"
)

type Config struct {
	RetryInterval time.Duration  `mapstructure:"retry_interval"`
	Client        *client.Config `mapstructure:"client"`
}

package redis

import "redis/lock"

type Config struct {
	Lock *lock.Config `mapstructure:"lock"`
}

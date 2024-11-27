package client

import (
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func NewRedisClient(log *zap.Logger, cfg *Config) (redis.UniversalClient, error) {
	tlsConfig, err := tlsConfig(cfg.TLSConfig)
	if err != nil {
		return nil, err
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:            cfg.Addrs,
		DB:               cfg.DB,
		Username:         cfg.Username,
		Password:         cfg.Password,
		SentinelPassword: cfg.SentinelPassword,
		MaxRetries:       cfg.MaxRetries,
		MinRetryBackoff:  cfg.MaxRetryBackoff,
		MaxRetryBackoff:  cfg.MaxRetryBackoff,
		DialTimeout:      cfg.DialTimeout,
		ReadTimeout:      cfg.ReadTimeout,
		WriteTimeout:     cfg.WriteTimeout,
		PoolSize:         cfg.PoolSize,
		MinIdleConns:     cfg.MinIdleConns,
		ConnMaxLifetime:  cfg.MaxConnAge,
		PoolTimeout:      cfg.PoolTimeout,
		ConnMaxIdleTime:  cfg.IdleTimeout,
		ReadOnly:         cfg.ReadOnly,
		RouteByLatency:   cfg.RouteByLatency,
		RouteRandomly:    cfg.RouteRandomly,
		MasterName:       cfg.MasterName,
		TLSConfig:        tlsConfig,
	})

	err = redisotel.InstrumentMetrics(client)
	if err != nil {
		log.Warn("failed to instrument redis metrics, driver will work without metrics", zap.Error(err))
	}

	err = redisotel.InstrumentTracing(client)
	if err != nil {
		log.Warn("failed to instrument redis tracing, driver will work without tracing", zap.Error(err))
	}

	return client, nil
}

package redis

import (
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
	"redis/lock"
)

const PluginName string = "redis"

type Plugin struct {
	log  *zap.Logger
	lock *lock.LockHandler
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

func (p *Plugin) Init(cfg Configurer, log Logger) error {
	const op = errors.Op("redis_plugin_init")
	if !cfg.Has(PluginName) {
		return errors.E(errors.Disabled)
	}

	var config *Config

	err := cfg.UnmarshalKey(PluginName, &config)
	if err != nil {
		return errors.E(op, err)
	}

	p.log = log.NamedLogger(PluginName)

	if config.Lock != nil {
		lockHandler, lErr := lock.NewLockHandler(config.Lock)
		if lErr != nil {
			return errors.E(op, lErr)
		}
		p.lock = lockHandler
	}

	return nil
}

func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) LockEnabled() bool {
	return p.lock != nil
}

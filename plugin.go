package redisqueue

import (
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"redisqueue/queue"
)

const (
	pluginName       string = "rr_redis_queue"
	masterPluginName string = "jobs"
)

type Plugin struct {
	log    *zap.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

type Configurer interface {
	UnmarshalKey(name string, out any) error
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) && !cfg.Has(masterPluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg

	return nil
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

func (p *Plugin) DriverFromConfig(configKey string, priorityQueue jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	return queue.FromConfig(p.tracer, configKey, pipeline, p.log, p.cfg, priorityQueue)
}

func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, priorityQueue jobs.Queue) (jobs.Driver, error) {
	return queue.FromPipeline(p.tracer, pipe, p.log, p.cfg, priorityQueue)
}

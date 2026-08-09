package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"redisqueue/client"

	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	pluginName string = "redisqueue"
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	UnmarshalKey(name string, out any) error
	Has(name string) bool
}

type Driver struct {
	mu            sync.Mutex
	log           *zap.Logger
	rdb           redis.UniversalClient
	priorityQueue jobs.Queue
	pipeline      atomic.Pointer[jobs.Pipeline]
	tracer        *sdktrace.TracerProvider

	channel         string
	visibilityLimit int
	prefetchLimit   int
	maxRetries      int
	retryDelay      int
	deadLetterTTL   int
	queuePrefix     string
	tickerInterval  time.Duration

	cancel context.CancelFunc

	listeners uint32
	stopped   uint64
}

func FromConfig(
	tracer *sdktrace.TracerProvider,
	configKey string,
	pipe jobs.Pipeline,
	log *zap.Logger,
	cfg Configurer,
	priorityQueue jobs.Queue,
) (*Driver, error) {
	const op = errors.Op("redisqueue_from_config")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration found for key: %s", configKey))
	}

	var queueConf Config
	err := cfg.UnmarshalKey(configKey, &queueConf)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to parse queue configuration: %w", err))
	}
	queueConf.InitDefaults()

	var connections map[string]client.Config
	if !cfg.Has("redisqueue.connections") {
		return nil, errors.E(op, errors.Str("no Redis connections found in configuration"))
	}
	err = cfg.UnmarshalKey("redisqueue.connections", &connections)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to parse Redis connections: %w", err))
	}

	clientConf, ok := connections[queueConf.Connection]
	if !ok {
		return nil, errors.E(op, fmt.Errorf("connection '%s' not found in global Redisqueue connections", queueConf.Connection))
	}
	clientConf.InitDefaults()

	rdb, err := client.NewRedisClient(log, &clientConf)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to initialize Redis client: %w", err))
	}

	driver := &Driver{
		log:             log,
		rdb:             rdb,
		priorityQueue:   priorityQueue,
		tracer:          tracer,
		channel:         queueConf.Channel,
		visibilityLimit: queueConf.VisibilityLimit,
		prefetchLimit:   queueConf.PrefetchLimit,
		maxRetries:      queueConf.MaxRetries,
		retryDelay:      queueConf.RetryDelay,
		deadLetterTTL:   queueConf.DeadLetterTTL,
		queuePrefix:     queueConf.QueuePrefix,
		tickerInterval:  queueConf.TickerInterval,
	}

	driver.pipeline.Store(&pipe)

	return driver, nil
}

func FromPipeline(
	tracer *sdktrace.TracerProvider,
	pipe jobs.Pipeline,
	log *zap.Logger,
	cfg Configurer,
	priorityQueue jobs.Queue,
) (*Driver, error) {
	const op = errors.Op("redis_pub_sub_pipeline")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	var connections map[string]client.Config
	if !cfg.Has("redisqueue.connections") {
		return nil, errors.E(op, errors.Str("no Redis connections found in configuration"))
	}
	err := cfg.UnmarshalKey("redisqueue.connections", &connections)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to parse Redis connections: %w", err))
	}

	var queueConf Config
	queueConf.Connection = pipe.String("connection", "")
	queueConf.QueuePrefix = pipe.String(queuePrefixKey, "")
	queueConf.Channel = pipe.String(channelKey, "")
	queueConf.PrefetchLimit = pipe.Int(prefetchLimitKey, 10)
	queueConf.VisibilityLimit = pipe.Int(visibilityTimeoutKey, 30)
	queueConf.MaxRetries = pipe.Int(maxRetriesKey, 1)
	queueConf.RetryDelay = pipe.Int(retryDelayKey, 5)
	queueConf.DeadLetterTTL = pipe.Int(deadLetterTTLKey, 7*24*60*60)

	tickerIntervalStr := pipe.String(tickerIntervalKey, "")
	var tickerInterval time.Duration

	if tickerIntervalStr == "" {
		tickerInterval = 0
	} else {
		duration, err := time.ParseDuration(tickerIntervalStr)
		if err != nil {
			return nil, errors.E(op, fmt.Errorf("invalid ticker_interval format: %w", err))
		}
		tickerInterval = duration
	}

	queueConf.TickerInterval = tickerInterval

	queueConf.InitDefaults()

	clientConf, ok := connections[queueConf.Connection]
	if !ok {
		return nil, errors.E(op, fmt.Errorf("connection '%s' not found in global Redisqueue connections", queueConf.Connection))
	}
	clientConf.InitDefaults()

	rdb, err := client.NewRedisClient(log, &clientConf)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to initialize Redis client: %w", err))
	}

	driver := &Driver{
		log:             log,
		rdb:             rdb,
		tracer:          tracer,
		priorityQueue:   priorityQueue,
		channel:         queueConf.Channel,
		visibilityLimit: queueConf.VisibilityLimit,
		prefetchLimit:   queueConf.PrefetchLimit,
		maxRetries:      queueConf.MaxRetries,
		retryDelay:      queueConf.RetryDelay,
		deadLetterTTL:   queueConf.DeadLetterTTL,
		queuePrefix:     queueConf.QueuePrefix,
		tickerInterval:  queueConf.TickerInterval,
	}

	driver.pipeline.Store(&pipe)

	return driver, nil
}

func (d *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("redisqueue_push")

	ctx, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_push")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != jb.GroupID() {
		return errors.E(op, fmt.Errorf("pipeline does not exist: %s, actual: %s", jb.GroupID(), pipe.Name()))
	}

	item := fromJob(jb)
	d.log.Debug("item autoack set to", zap.Bool("autoAck", item.Options.AutoAck))
	redisStringData, err := json.Marshal(item)
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to JSON encode payload: %w", err))
	}

	result, err := d.store(ctx, item, string(redisStringData))
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to atomically store task: %w", err))
	}

	if result == transitionResultTerminal {
		return errors.E(op, fmt.Errorf("refusing to overwrite orphaned payload for task %s", item.ID()))
	}

	if result == transitionResultNoop {
		d.log.Debug("duplicate task push ignored", zap.String("taskID", item.ID()))
		return nil
	}

	if item.Options.DelayDuration() <= 0 {
		if publishError := d.rdb.Publish(ctx, d.channel, item.GroupID()).Err(); publishError != nil {
			d.log.Warn("task stored but queue notification failed", zap.String("taskID", item.ID()), zap.Error(publishError))
		}
	}

	d.log.Debug("task stored", zap.String("queue", item.GroupID()), zap.String("taskID", item.ID()))

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("redis_pub_sub_run")

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_run")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()
	if atomic.LoadUint64(&d.stopped) == 1 {
		return errors.E(op, errors.Str("pipeline is stopped"))
	}

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, fmt.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	if !atomic.CompareAndSwapUint32(&d.listeners, 0, 1) {
		return errors.E(op, errors.Str("listener is already in the active state"))
	}
	d.listen()

	d.log.Debug("Pipeline started", zap.String("pipeline", p.Name()))

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_state")
	defer span.End()

	pipe := *d.pipeline.Load()

	keys := d.keys(pipe.Name())
	delayed, err := d.rdb.ZCard(ctx, keys.delayed).Result()
	if err != nil {
		return nil, err
	}
	reserved, err := d.rdb.ZCard(ctx, keys.reserved).Result()
	if err != nil {
		return nil, err
	}
	ready, err := d.rdb.ZCard(ctx, keys.ready).Result()
	if err != nil {
		return nil, err
	}

	return &jobs.State{
		Pipeline: pipe.Name(),
		Driver:   pluginName,
		Queue:    d.channel,
		Active:   reserved,
		Delayed:  delayed,
		Reserved: ready,
		Ready:    atomic.LoadUint32(&d.listeners) == 1,
	}, nil
}

func (d *Driver) Pause(ctx context.Context, pipelineName string) error {
	start := time.Now()

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_pause")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != pipelineName {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	listeners := atomic.LoadUint32(&d.listeners)

	if listeners == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	d.log.Debug("stop listening for messages",
		zap.String("driver", pipe.Driver()),
		zap.String("pipeline", pipe.Name()),
		zap.Time("start", start),
	)

	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	atomic.StoreUint32(&d.listeners, 0)

	d.log.Debug("pipeline was paused",
		zap.String("driver", pipe.Driver()),
		zap.String("pipeline", pipe.Name()),
		zap.Time("end", time.Now().UTC()),
		zap.Duration("elapsed", time.Since(start)),
	)

	return nil
}

func (d *Driver) Resume(ctx context.Context, pipelineName string) error {
	start := time.Now().UTC()

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_resume")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()
	if atomic.LoadUint64(&d.stopped) == 1 {
		return errors.Str("pipeline is stopped")
	}

	pipe := *d.pipeline.Load()
	if pipe.Name() != pipelineName {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	listeners := atomic.LoadUint32(&d.listeners)
	if listeners == 1 {
		return errors.Str("listener is already in the active state")
	}

	d.log.Debug("resume listening for messages",
		zap.String("driver", pipe.Driver()),
		zap.String("pipeline", pipe.Name()),
		zap.Time("start", start),
	)

	if !atomic.CompareAndSwapUint32(&d.listeners, 0, 1) {
		return errors.Str("listener is already in the active state")
	}
	d.listen()

	d.log.Debug("pipeline was resumed",
		zap.String("driver", pipe.Driver()),
		zap.String("pipeline", pipe.Name()),
		zap.Time("end", time.Now().UTC()),
		zap.Duration("elapsed", time.Since(start)),
	)

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "redisqueue_stop")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()

	d.log.Debug("stopping pipeline", zap.String("driver", pluginName), zap.Time("start", start))

	atomic.StoreUint64(&d.stopped, 1)
	atomic.StoreUint32(&d.listeners, 0)
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}

	err := d.rdb.Close()
	if err != nil {
		d.log.Warn("failed to close Redis client", zap.Error(err))
	}

	d.log.Debug("pipeline was stopped",
		zap.String("driver", pluginName),
		zap.Time("end", time.Now().UTC()),
		zap.Duration("elapsed", time.Since(start)),
	)

	return nil
}

func (d *Driver) handleRequeue(ctx context.Context, job *Item, delay int) error {
	const op = errors.Op("redisqueue_requeue")
	redisStringData, err := json.Marshal(job)
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to JSON encode payload: %w", err))
	}

	result, err := d.requeue(ctx, job, string(redisStringData), delay)
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to atomically requeue task: %w", err))
	}

	if result == transitionResultApplied && delay <= 0 {
		if publishError := d.rdb.Publish(ctx, d.channel, job.GroupID()).Err(); publishError != nil {
			d.log.Warn("task requeued but queue notification failed", zap.String("taskID", job.ID()), zap.Error(publishError))
		}
	}

	if result == transitionResultNoop {
		d.log.Warn("stale task requeue ignored", zap.String("taskID", job.ID()))
	}

	if result == transitionResultTerminal {
		d.log.Error("task exhausted its delivery limit and was dead-lettered", zap.String("taskID", job.ID()))
	}

	return nil
}

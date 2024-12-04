package queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/events"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"redisqueue/client"
	"sync"
	"sync/atomic"
	"time"
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
	prop          propagation.TextMapPropagator

	channel         string
	visibilityLimit int
	prefetchLimit   int
	maxRetries      int
	queuePrefix     string

	eventsChannel chan events.Event
	eventBus      *events.Bus
	id            string
	ctx           context.Context
	cancel        context.CancelFunc

	pubSub    *redis.PubSub
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
		return nil, errors.E(op, errors.Errorf("No configuration found for key: %s", configKey))
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

	ctx, cancel := context.WithCancel(context.Background())

	eventsChannel := make(chan events.Event, 1)
	eventBus, id := events.NewEventBus()

	driver := &Driver{
		log:             log,
		rdb:             rdb,
		priorityQueue:   priorityQueue,
		tracer:          tracer,
		prop:            prop,
		channel:         queueConf.Channel,
		visibilityLimit: queueConf.VisibilityLimit,
		prefetchLimit:   queueConf.PrefetchLimit,
		maxRetries:      queueConf.MaxRetries,
		queuePrefix:     queueConf.QueuePrefix,
		eventsChannel:   eventsChannel,
		eventBus:        eventBus,
		id:              id,
		ctx:             ctx,
		cancel:          cancel,
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
	if !cfg.Has("requeue.connections") {
		return nil, errors.E(op, errors.Str("no Redis connections found in configuration"))
	}
	err := cfg.UnmarshalKey("redisqueue.connections", &connections)
	if err != nil {
		return nil, errors.E(op, fmt.Errorf("failed to parse Redis connections: %w", err))
	}

	var queueConf Config
	queueConf.Connection = pipe.String("connection", "")
	queueConf.QueuePrefix = pipe.String("queue_prefix", "")
	queueConf.Channel = pipe.String("channel", "")
	queueConf.PrefetchLimit = pipe.Int("prefetch_limit", 10)
	queueConf.VisibilityLimit = pipe.Int("visibility_limit", 30)
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

	ctx, cancel := context.WithCancel(context.Background())

	eventsChannel := make(chan events.Event, 1)
	eventBus, id := events.NewEventBus()

	driver := &Driver{
		log:             log,
		rdb:             rdb,
		tracer:          tracer,
		prop:            prop,
		priorityQueue:   priorityQueue,
		channel:         queueConf.Channel,
		visibilityLimit: queueConf.VisibilityLimit,
		prefetchLimit:   queueConf.PrefetchLimit,
		maxRetries:      queueConf.MaxRetries,
		queuePrefix:     queueConf.QueuePrefix,
		ctx:             ctx,
		cancel:          cancel,
		eventsChannel:   eventsChannel,
		eventBus:        eventBus,
		id:              id,
	}

	driver.pipeline.Store(&pipe)

	return driver, nil
}

func (d *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("redis_pub_sub_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_push")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != jb.GroupID() {
		return errors.E(op, fmt.Errorf("pipeline does not exist: %s, actual: %s", jb.GroupID(), pipe.Name()))
	}

	taskID := fmt.Sprintf("%s%d", pipe.Name(), time.Now().UnixNano())

	err := d.rdb.Set(ctx, taskID, jb.Payload(), 0).Err()
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to store payload: %w", err))
	}

	queueKey := fmt.Sprintf("%s_tasks", pipe.Name())
	err = d.rdb.ZAdd(ctx, queueKey, redis.Z{
		Score:  float64(jb.Priority()),
		Member: taskID,
	}).Err()
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to add task to sorted set: %w", err))
	}

	err = d.rdb.Publish(ctx, d.channel, pipe.Name()).Err()
	if err != nil {
		return errors.E(op, fmt.Errorf("failed to publish queue name: %w", err))
	}

	d.log.Debug("Task pushed and queue notified", zap.String("queue", pipe.Name()), zap.String("taskID", taskID))

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("redis_pub_sub_run")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_run")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, fmt.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	atomic.AddUint32(&d.listeners, 1)
	d.listen()

	d.log.Debug("Pipeline started", zap.String("pipeline", p.Name()))

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_state")
	defer span.End()

	pipe := *d.pipeline.Load()

	active := int64(0)
	if atomic.LoadUint32(&d.listeners) == 1 {
		active = 1
	}

	return &jobs.State{
		Pipeline: pipe.Name(),
		Driver:   pluginName,
		Queue:    d.channel,
		Active:   active,
		Ready:    d.priorityQueue.Len() > 0,
	}, nil
}

func (d *Driver) Pause(ctx context.Context, pipelineName string) error {
	start := time.Now()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_pause")
	defer span.End()

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

	d.checkCtxAndCancel()
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

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_resume")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

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

	d.atomicCtx()
	d.listen()
	atomic.StoreUint32(&d.listeners, 1)

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

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "redisqueue_stop")
	defer span.End()

	d.log.Debug("stopping pipeline", zap.String("driver", pluginName), zap.Time("start", start))

	d.checkCtxAndCancel()

	if d.pubSub != nil {
		err := d.pubSub.Close()
		if err != nil {
			d.log.Warn("failed to close Pub/Sub subscription", zap.Error(err))
		}
	}

	err := d.rdb.Close()
	if err != nil {
		d.log.Warn("failed to close Redis client", zap.Error(err))
	}

	atomic.StoreUint64(&d.stopped, 1)

	d.log.Debug("pipeline was stopped",
		zap.String("driver", pluginName),
		zap.Time("end", time.Now().UTC()),
		zap.Duration("elapsed", time.Since(start)),
	)

	return nil
}

func (d *Driver) requeueTask(ctx context.Context, item *Item) error {
	queueKey := fmt.Sprintf("%s_%s", d.queuePrefix, item.GroupID())

	err := d.rdb.ZAdd(ctx, queueKey, redis.Z{
		Score:  float64(item.Priority()),
		Member: item.ID(),
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to requeue task: %w", err)
	}

	d.log.Debug("task requeued", zap.String("queue", queueKey), zap.String("taskID", item.ID()))
	return nil
}

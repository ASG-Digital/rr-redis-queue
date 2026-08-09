package queue

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

func TestStoreIsAtomicAndDuplicatePushIsIdempotent(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("task-1")

	result := storeTestItem(t, driver, item)
	if result != transitionResultApplied {
		t.Fatalf("expected task to be stored, got %d", result)
	}

	result = storeTestItem(t, driver, item)
	if result != transitionResultNoop {
		t.Fatalf("expected duplicate task to be ignored, got %d", result)
	}

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.ready); count != 1 {
		t.Fatalf("expected one ready task, got %d", count)
	}
	if !redisServer.Exists(item.ID()) {
		t.Fatal("expected payload to be stored")
	}
}

func TestStoreRejectsExistingOrphanPayload(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("orphan-task")
	redisServer.Set(item.ID(), "old payload")

	result := storeTestItem(t, driver, item)
	if result != transitionResultTerminal {
		t.Fatalf("expected orphan payload collision, got %d", result)
	}
}

func TestStoreRecoversIdenticalOrphanPayload(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("recoverable-orphan")
	payload := marshalTestItem(t, item)
	redisServer.Set(item.ID(), payload)

	result, err := driver.store(context.Background(), item, payload)
	if err != nil {
		t.Fatalf("recover orphan task: %v", err)
	}
	if result != transitionResultApplied {
		t.Fatalf("expected identical orphan payload to be recovered, got %d", result)
	}

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.ready); count != 1 {
		t.Fatalf("expected recovered task in ready queue, got %d", count)
	}
}

func TestStoreKeepsInitiallyDelayedTaskUnavailable(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("initially-delayed")
	item.Options.Delay = 60
	storeTestItem(t, driver, item)

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.ready); count != 0 {
		t.Fatalf("expected no ready tasks, got %d", count)
	}
	if count := zCard(t, redisServer, keys.delayed); count != 1 {
		t.Fatalf("expected one delayed task, got %d", count)
	}

	reserved, err := driver.reserve(context.Background(), item.GroupID(), 1)
	if err != nil {
		t.Fatalf("reserve delayed task: %v", err)
	}
	if len(reserved) != 0 {
		t.Fatalf("expected delayed task to remain unavailable, got %d tasks", len(reserved))
	}
}

func TestReservePreservesPriorityForTasksCreatedByPreviousPluginVersion(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("legacy-ready-task")
	payload := marshalTestItem(t, item)
	keys := driver.keys(item.GroupID())
	redisServer.Set(item.ID(), payload)
	redisServer.ZAdd(keys.ready, 3, item.ID())

	reserveOne(t, driver, item.GroupID())
	priority, err := driver.rdb.HGet(context.Background(), keys.priorities, item.ID()).Result()
	if err != nil {
		t.Fatalf("read preserved priority: %v", err)
	}
	if priority != "3" {
		t.Fatalf("expected priority 3 to be preserved, got %s", priority)
	}
}

func TestReserveReturnsHighestPriorityTaskFirst(t *testing.T) {
	driver, _ := newTestDriver(t)
	lowPriority := newTestItem("low-priority")
	lowPriority.Options.Priority = 20
	highPriority := newTestItem("high-priority")
	highPriority.Options.Priority = 1
	storeTestItem(t, driver, lowPriority)
	storeTestItem(t, driver, highPriority)

	reserved, err := driver.reserve(context.Background(), highPriority.GroupID(), 2)
	if err != nil {
		t.Fatalf("reserve prioritized tasks: %v", err)
	}
	if len(reserved) != 2 {
		t.Fatalf("expected two reserved tasks, got %d", len(reserved))
	}
	if reserved[0].id != highPriority.ID() || reserved[1].id != lowPriority.ID() {
		t.Fatalf("unexpected priority order: %s, %s", reserved[0].id, reserved[1].id)
	}
}

func TestAckRemovesAllActiveTaskDataIncludingAutoAck(t *testing.T) {
	for _, autoAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual_ack", true: "auto_ack"}[autoAck], func(t *testing.T) {
			driver, redisServer := newTestDriver(t)
			item := newTestItem("ack-task")
			item.Options.AutoAck = autoAck
			storeTestItem(t, driver, item)

			reserved := reserveOne(t, driver, item.GroupID())
			if err := reserved.Ack(); err != nil {
				t.Fatalf("ack failed: %v", err)
			}

			keys := driver.keys(item.GroupID())
			if redisServer.Exists(item.ID()) {
				t.Fatal("expected payload to be deleted")
			}
			if count := zCard(t, redisServer, keys.reserved); count != 0 {
				t.Fatalf("expected no reserved tasks, got %d", count)
			}
		})
	}
}

func TestInfrastructureNackHasBoundedRetries(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	driver.maxRetries = 1
	driver.retryDelay = 0
	item := newTestItem("poison-task")
	storeTestItem(t, driver, item)

	firstDelivery := reserveOne(t, driver, item.GroupID())
	if err := firstDelivery.Nack(); err != nil {
		t.Fatalf("first nack failed: %v", err)
	}

	secondDelivery := reserveOne(t, driver, item.GroupID())
	if secondDelivery.deliveryCount != 2 {
		t.Fatalf("expected second delivery, got %d", secondDelivery.deliveryCount)
	}
	if err := secondDelivery.Nack(); err != nil {
		t.Fatalf("second nack failed: %v", err)
	}

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.ready); count != 0 {
		t.Fatalf("expected exhausted task to leave ready queue, got %d", count)
	}
	if count := zCard(t, redisServer, keys.reserved); count != 0 {
		t.Fatalf("expected exhausted task to leave reserved queue, got %d", count)
	}
	if count := zCard(t, redisServer, keys.dead); count != 1 {
		t.Fatalf("expected exhausted task in dead-letter queue, got %d", count)
	}
}

func TestExplicitDiscardDoesNotRetry(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("discard-task")
	storeTestItem(t, driver, item)

	reserved := reserveOne(t, driver, item.GroupID())
	if err := reserved.NackWithOptions(false, 0); err != nil {
		t.Fatalf("discard failed: %v", err)
	}

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.ready); count != 0 {
		t.Fatalf("expected discarded task not to be retried, got %d", count)
	}
	if count := zCard(t, redisServer, keys.dead); count != 1 {
		t.Fatalf("expected discarded task in dead-letter queue, got %d", count)
	}
}

func TestDelayedRequeueIsNotReservedEarly(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("delayed-task")
	storeTestItem(t, driver, item)

	reserved := reserveOne(t, driver, item.GroupID())
	if err := reserved.Requeue(nil, 60); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}

	if items, err := driver.reserve(context.Background(), item.GroupID(), 1); err != nil || len(items) != 0 {
		t.Fatalf("expected delayed task to remain unavailable, items=%d error=%v", len(items), err)
	}

	keys := driver.keys(item.GroupID())
	redisServer.ZAdd(keys.delayed, -1, item.ID())
	secondDelivery := reserveOne(t, driver, item.GroupID())
	if secondDelivery.deliveryCount != 2 {
		t.Fatalf("expected delayed task's second delivery, got %d", secondDelivery.deliveryCount)
	}
}

func TestExpiredReservationRetriesAndRejectsStaleAck(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	driver.maxRetries = 1
	item := newTestItem("expired-task")
	storeTestItem(t, driver, item)

	firstDelivery := reserveOne(t, driver, item.GroupID())
	keys := driver.keys(item.GroupID())
	redisServer.ZAdd(keys.reserved, -1, item.ID())
	secondDelivery := reserveOne(t, driver, item.GroupID())

	if err := firstDelivery.Ack(); err != nil {
		t.Fatalf("stale ack returned an error: %v", err)
	}
	if !redisServer.Exists(item.ID()) {
		t.Fatal("stale ack removed the current delivery")
	}

	if err := secondDelivery.Ack(); err != nil {
		t.Fatalf("current ack failed: %v", err)
	}
	if redisServer.Exists(item.ID()) {
		t.Fatal("current ack did not remove payload")
	}
}

func TestExpiredReservationRejectsStaleRequeue(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	driver.maxRetries = 1
	driver.retryDelay = 0
	item := newTestItem("stale-requeue-task")
	storeTestItem(t, driver, item)

	firstDelivery := reserveOne(t, driver, item.GroupID())
	keys := driver.keys(item.GroupID())
	redisServer.ZAdd(keys.reserved, -1, item.ID())
	secondDelivery := reserveOne(t, driver, item.GroupID())

	if err := firstDelivery.Nack(); err != nil {
		t.Fatalf("stale nack returned an error: %v", err)
	}
	if count := zCard(t, redisServer, keys.reserved); count != 1 {
		t.Fatalf("expected current delivery to remain reserved, got %d", count)
	}
	if count := zCard(t, redisServer, keys.ready); count != 0 {
		t.Fatalf("expected stale delivery not to be requeued, got %d", count)
	}

	if err := secondDelivery.Ack(); err != nil {
		t.Fatalf("current ack failed: %v", err)
	}
}

func TestMalformedPayloadIsDeadLetteredOnce(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("malformed-task")
	storeTestItem(t, driver, item)
	redisServer.Set(item.ID(), "not-json")

	if err := driver.processQueue(context.Background(), item.GroupID()); err != nil {
		t.Fatalf("processing malformed task failed: %v", err)
	}

	keys := driver.keys(item.GroupID())
	if count := zCard(t, redisServer, keys.dead); count != 1 {
		t.Fatalf("expected malformed task in dead-letter queue, got %d", count)
	}
	if driver.priorityQueue.Len() != 0 {
		t.Fatal("malformed task reached RoadRunner priority queue")
	}

	if err := driver.processQueue(context.Background(), item.GroupID()); err != nil {
		t.Fatalf("second malformed task scan failed: %v", err)
	}
	if count := zCard(t, redisServer, keys.dead); count != 1 {
		t.Fatalf("expected malformed task to remain dead-lettered once, got %d", count)
	}
}

func TestExpiredDeadLetterMetadataIsCleanedBeforeReservation(t *testing.T) {
	driver, redisServer := newTestDriver(t)
	item := newTestItem("expired-dead-letter")
	storeTestItem(t, driver, item)

	reserved := reserveOne(t, driver, item.GroupID())
	if err := reserved.NackWithOptions(false, 0); err != nil {
		t.Fatalf("discard failed: %v", err)
	}

	keys := driver.keys(item.GroupID())
	redisServer.ZAdd(keys.dead, -1, item.ID())
	items, err := driver.reserve(context.Background(), item.GroupID(), 1)
	if err != nil {
		t.Fatalf("reserve after dead-letter expiry failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no reserved tasks, got %d", len(items))
	}
	if count := zCard(t, redisServer, keys.dead); count != 0 {
		t.Fatalf("expected expired dead-letter entry to be removed, got %d", count)
	}

	for _, metadataKey := range []string{keys.priorities, keys.deliveries, keys.tokens} {
		exists, existsError := driver.rdb.HExists(context.Background(), metadataKey, item.ID()).Result()
		if existsError != nil {
			t.Fatalf("read metadata %s: %v", metadataKey, existsError)
		}
		if exists {
			t.Fatalf("expected expired metadata to be removed from %s", metadataKey)
		}
	}
}

func TestListenerStopsPollingWhilePausedAndContinuesAfterResume(t *testing.T) {
	driver, _ := newTestDriver(t)
	driver.tickerInterval = 5 * time.Millisecond
	pipeline := jobs.Pipeline(&testPipeline{name: "registration-fee", driver: pluginName})
	driver.pipeline.Store(&pipeline)

	if err := driver.Run(context.Background(), pipeline); err != nil {
		t.Fatalf("run listener: %v", err)
	}
	t.Cleanup(func() {
		driver.mu.Lock()
		if driver.cancel != nil {
			driver.cancel()
		}
		driver.mu.Unlock()
	})

	first := newTestItem("polled-task")
	storeTestItem(t, driver, first)
	waitForQueueLength(t, driver.priorityQueue, 1)

	if err := driver.Pause(context.Background(), pipeline.Name()); err != nil {
		t.Fatalf("pause listener: %v", err)
	}
	second := newTestItem("paused-task")
	storeTestItem(t, driver, second)
	time.Sleep(5 * driver.tickerInterval)
	if length := driver.priorityQueue.Len(); length != 1 {
		t.Fatalf("expected paused listener to leave queue length at 1, got %d", length)
	}

	if err := driver.Resume(context.Background(), pipeline.Name()); err != nil {
		t.Fatalf("resume listener: %v", err)
	}
	waitForQueueLength(t, driver.priorityQueue, 2)
}

func newTestDriver(t *testing.T) (*Driver, *miniredis.Miniredis) {
	t.Helper()

	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	return &Driver{
		log:             zap.NewNop(),
		rdb:             redisClient,
		priorityQueue:   &testPriorityQueue{},
		tracer:          sdktrace.NewTracerProvider(),
		visibilityLimit: 30,
		prefetchLimit:   10,
		maxRetries:      1,
		retryDelay:      0,
		deadLetterTTL:   60,
		queuePrefix:     "test",
	}, redisServer
}

func newTestItem(id string) *Item {
	return &Item{
		Job:        "TestJob",
		Ident:      id,
		Payload:    []byte(`{"id":1}`),
		HeadersMap: map[string][]string{},
		Options: &Options{
			Priority: 10,
			Pipeline: "registration-fee",
		},
	}
}

func storeTestItem(t *testing.T, driver *Driver, item *Item) int64 {
	t.Helper()

	result, err := driver.store(context.Background(), item, marshalTestItem(t, item))
	if err != nil {
		t.Fatalf("store task: %v", err)
	}

	return result
}

func marshalTestItem(t *testing.T, item *Item) string {
	t.Helper()

	payload, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}

	return string(payload)
}

func reserveOne(t *testing.T, driver *Driver, pipeline string) *Item {
	t.Helper()

	reserved, err := driver.reserve(context.Background(), pipeline, 1)
	if err != nil {
		t.Fatalf("reserve task: %v", err)
	}
	if len(reserved) != 1 {
		t.Fatalf("expected one reserved task, got %d", len(reserved))
	}

	var item Item
	if err := json.Unmarshal([]byte(reserved[0].payload), &item); err != nil {
		t.Fatalf("unmarshal reserved task: %v", err)
	}
	driver.addItemRuntime(&item, reserved[0])

	return &item
}

func zCard(t *testing.T, redisServer *miniredis.Miniredis, key string) int {
	t.Helper()

	members, err := redisServer.ZMembers(key)
	if err != nil {
		if !redisServer.Exists(key) {
			return 0
		}
		t.Fatalf("read sorted set %s: %v", key, err)
	}

	return len(members)
}

func waitForQueueLength(t *testing.T, queue jobs.Queue, expected uint64) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if queue.Len() == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("expected queue length %d, got %d", expected, queue.Len())
}

type testPriorityQueue struct {
	mu    sync.Mutex
	items []jobs.Job
}

func (q *testPriorityQueue) Remove(id string) []jobs.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	removed := make([]jobs.Job, 0)
	remaining := make([]jobs.Job, 0, len(q.items))
	for _, item := range q.items {
		if item.ID() == id {
			removed = append(removed, item)
		} else {
			remaining = append(remaining, item)
		}
	}
	q.items = remaining

	return removed
}

func (q *testPriorityQueue) Insert(item jobs.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, item)
}

func (q *testPriorityQueue) ExtractMin() jobs.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	q.items = q.items[1:]

	return item
}

func (q *testPriorityQueue) Len() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	return uint64(len(q.items))
}

type testPipeline struct {
	name   string
	driver string
	values map[string]any
}

func (p *testPipeline) With(name string, value interface{}) {
	if p.values == nil {
		p.values = make(map[string]any)
	}
	p.values[name] = value
}

func (p *testPipeline) Name() string {
	return p.name
}

func (p *testPipeline) Driver() string {
	return p.driver
}

func (p *testPipeline) Has(name string) bool {
	_, exists := p.values[name]
	return exists
}

func (p *testPipeline) String(name string, defaultValue string) string {
	value, exists := p.values[name]
	if !exists {
		return defaultValue
	}

	stringValue, valid := value.(string)
	if !valid {
		return defaultValue
	}

	return stringValue
}

func (p *testPipeline) Int(name string, defaultValue int) int {
	value, exists := p.values[name]
	if !exists {
		return defaultValue
	}

	switch typedValue := value.(type) {
	case int:
		return typedValue
	case int64:
		return int(typedValue)
	case string:
		parsed, err := strconv.Atoi(typedValue)
		if err == nil {
			return parsed
		}
	}

	return defaultValue
}

func (p *testPipeline) Bool(name string, defaultValue bool) bool {
	value, exists := p.values[name]
	if !exists {
		return defaultValue
	}

	boolValue, valid := value.(bool)
	if !valid {
		return defaultValue
	}

	return boolValue
}

func (p *testPipeline) Map(_ string, _ map[string]string) error {
	return nil
}

func (p *testPipeline) Priority() int64 {
	return 10
}

func (p *testPipeline) Get(name string) interface{} {
	return p.values[name]
}

var _ jobs.Queue = (*testPriorityQueue)(nil)

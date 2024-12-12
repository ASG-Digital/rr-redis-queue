package queue

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/events"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

const (
	restartStr string = "restart"
)

func (d *Driver) listen() {
	d.atomicCtx()

	go func() {
		d.log.Debug("subscribing to channel", zap.String("channel", d.channel))
		d.pubSub = d.rdb.Subscribe(d.ctx, d.channel)
		defer func(pubSub *redis.PubSub) {
			_ = pubSub.Close()
		}(d.pubSub)
		ch := d.pubSub.Channel()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		pipe := *d.pipeline.Load()
		queueName := pipe.Name()

		for {
			select {
			case <-d.ctx.Done():
				atomic.StoreUint32(&d.listeners, 0)
				d.log.Debug("listener stopped", zap.String("channel", d.channel))
				return
			case msg := <-ch:
				if msg == nil {
					d.log.Warn("received nil message, skipping")
					continue
				}
				queueNameFromMessage := msg.Payload
				d.log.Debug("received message", zap.String("queue", queueNameFromMessage))

				err := d.processQueue(queueNameFromMessage)
				if err != nil {
					d.handleProcessingError(err, queueNameFromMessage)
				}
			case <-ticker.C:
				d.log.Debug("ticker triggered, processing any pending tasks")

				err := d.processQueue(queueName)
				if err != nil {
					d.handleProcessingError(err, queueName)
				}
			}

		}
	}()
}

func (d *Driver) processQueue(queueName string) error {
	queueKey := d.queuePrefix + "_" + queueName
	var unprocessedTasks []redis.Z

	for {
		select {
		case <-d.ctx.Done():
			if len(unprocessedTasks) > 0 {
				d.log.Warn("context cancelled, reinserting unprocessed tasks", zap.String("queue", queueName), zap.Int("jobs_to_reinsert", len(unprocessedTasks)))
				_, err := d.rdb.ZAdd(d.ctx, queueKey, unprocessedTasks...).Result()
				if err != nil {
					d.log.Error("failed to reinsert unprocessed tasks", zap.Error(err), zap.String("queue", queueName))
				}
			}
			return d.ctx.Err()
		default:
			if d.priorityQueue.Len() >= uint64(d.prefetchLimit) {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			spaceAvailable := int64(d.prefetchLimit) - int64(d.priorityQueue.Len())

			tasks, err := d.rdb.ZPopMin(d.ctx, queueKey, spaceAvailable).Result()
			if err != nil {
				return err
			}

			if len(tasks) == 0 {
				d.log.Debug("no tasks found in queue", zap.String("queue", queueName))
				return nil
			}

			for _, task := range tasks {
				taskID := task.Member.(string)

				taskJson, err := d.rdb.Get(d.ctx, taskID).Result()
				if err != nil {
					d.log.Error("failed to fetch task payload", zap.String("taskID", taskID), zap.Error(err))
					unprocessedTasks = append(unprocessedTasks, redis.Z{Score: task.Score, Member: task.Member})
					continue
				}

				var item Item

				err = json.Unmarshal([]byte(taskJson), &item)
				if err != nil {
					unprocessedTasks = append(unprocessedTasks, redis.Z{Score: task.Score, Member: task.Member})
					d.log.Debug("Unmarshalling failed")
					continue
				}

				d.addItemOptions(&item)
				d.priorityQueue.Insert(&item)
				d.log.Debug("task pushed to priority queue", zap.Uint64("queue size", d.priorityQueue.Len()))
			}

			if len(unprocessedTasks) > 0 {
				_, err := d.rdb.ZAdd(d.ctx, queueKey, unprocessedTasks...).Result()
				if err != nil {
					d.log.Error("failed to reinsert unprocessed tasks", zap.Error(err), zap.String("queue", queueName))
				} else {
					d.log.Debug("reinserted unprocessed tasks", zap.String("queue", queueName), zap.Int("jobs_reinserted", len(unprocessedTasks)))
				}
				unprocessedTasks = nil
			}
		}
	}
}

func (d *Driver) handleProcessingError(err error, queueName string) {
	if errors.Is(err, context.Canceled) {
		atomic.StoreUint32(&d.listeners, 0)
		return
	}

	if errors.Is(err, redis.ErrClosed) {
		if atomic.LoadUint32(&d.listeners) > 0 {
			atomic.AddUint32(&d.listeners, ^uint32(0))
		}
		d.log.Debug("listener was stopped")
		return
	}

	atomic.StoreUint32(&d.listeners, 0)

	if atomic.LoadUint64(&d.stopped) == 1 {
		return
	}

	pipe := (*d.pipeline.Load()).Name()
	d.eventsChannel <- events.NewEvent(events.EventJOBSDriverCommand, pipe, restartStr)
	d.log.Error("process error, restarting the pipeline", zap.Error(err), zap.String("pipeline", pipe))
}

func (d *Driver) atomicCtx() {
	d.ctx, d.cancel = context.WithCancel(context.Background())
}

func (d *Driver) checkCtxAndCancel() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.cancel != nil {
		d.cancel()
	}
}

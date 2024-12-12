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
	tasks, err := d.rdb.ZPopMin(d.ctx, queueKey, int64(d.prefetchLimit)).Result()
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
			continue
		}

		var item Item

		err = json.Unmarshal([]byte(taskJson), &item)
		if err != nil {
			d.log.Debug("Unmarshalling failed")
		}

		d.addItemOptions(&item)
		d.priorityQueue.Insert(&item)
		d.log.Debug("task pushed to priority queue", zap.Uint64("queue size", d.priorityQueue.Len()))
	}

	return nil
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

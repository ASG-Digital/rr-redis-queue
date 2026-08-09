package queue

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"
)

func (d *Driver) listen() {
	listenerContext, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go func(ctx context.Context) {
		d.log.Debug("subscribing to channel", zap.String("channel", d.channel))
		pubSub := d.rdb.Subscribe(ctx, d.channel)
		messages := pubSub.Channel()
		defer func() {
			if pubSub != nil {
				_ = pubSub.Close()
			}
		}()

		ticker := time.NewTicker(d.tickerInterval)
		defer ticker.Stop()

		pipe := *d.pipeline.Load()
		queueName := pipe.Name()

		for {
			select {
			case <-ctx.Done():
				d.log.Debug("listener stopped", zap.String("channel", d.channel))
				return
			case msg, open := <-messages:
				if !open {
					d.log.Warn("Redis Pub/Sub channel closed; durable polling remains active")
					if pubSub != nil {
						_ = pubSub.Close()
					}
					pubSub = nil
					messages = nil
					continue
				}
				if msg == nil {
					d.log.Warn("received nil Redis Pub/Sub message")
					continue
				}
				if msg.Payload != queueName {
					d.log.Warn("ignoring notification for another queue",
						zap.String("expected_queue", queueName),
						zap.String("received_queue", msg.Payload),
					)
					continue
				}
				d.log.Debug("received message", zap.String("queue", queueName))

				err := d.processQueue(ctx, queueName)
				if err != nil {
					d.log.Error("failed to process durable queue after notification; next poll will retry", zap.Error(err))
				}
			case <-ticker.C:
				d.log.Debug("ticker triggered, processing any pending tasks")
				if messages == nil {
					d.log.Debug("resubscribing to Redis notification channel", zap.String("channel", d.channel))
					pubSub = d.rdb.Subscribe(ctx, d.channel)
					messages = pubSub.Channel()
				}

				err := d.processQueue(ctx, queueName)
				if err != nil {
					d.log.Error("failed to process durable queue; next poll will retry", zap.Error(err))
				}
			}

		}
	}(listenerContext)
}

func (d *Driver) processQueue(ctx context.Context, queueName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if d.priorityQueue.Len() >= uint64(d.prefetchLimit) {
		return nil
	}

	spaceAvailable := int64(d.prefetchLimit) - int64(d.priorityQueue.Len())
	tasks, err := d.reserve(ctx, queueName, spaceAvailable)
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		d.log.Debug("no tasks found in queue", zap.String("queue", queueName))
		return nil
	}

	for _, task := range tasks {
		var item Item
		if unmarshalError := json.Unmarshal([]byte(task.payload), &item); unmarshalError != nil {
			d.log.Error("task payload is malformed and was dead-lettered", zap.String("taskID", task.id), zap.Error(unmarshalError))
			if discardError := d.discardMalformed(ctx, queueName, task.id); discardError != nil {
				return discardError
			}
			continue
		}

		if item.ID() != task.id || item.Options == nil || item.GroupID() != queueName {
			d.log.Error("task metadata is inconsistent and was dead-lettered", zap.String("taskID", task.id))
			if discardError := d.discardMalformed(ctx, queueName, task.id); discardError != nil {
				return discardError
			}
			continue
		}

		d.addItemRuntime(&item, task)
		d.priorityQueue.Insert(&item)
		d.log.Debug("task reserved and pushed to RoadRunner priority queue",
			zap.String("taskID", task.id),
			zap.Int("delivery", task.deliveries),
			zap.Uint64("queue_size", d.priorityQueue.Len()),
		)
	}

	return nil
}

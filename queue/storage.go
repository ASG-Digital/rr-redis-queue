package queue

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

const (
	transitionResultNoop     = 0
	transitionResultApplied  = 1
	transitionResultTerminal = -1
)

type queueKeys struct {
	ready      string
	delayed    string
	reserved   string
	dead       string
	priorities string
	deliveries string
	tokens     string
}

type reservedItem struct {
	id         string
	payload    string
	token      string
	deliveries int
}

func (d *Driver) keys(pipeline string) queueKeys {
	ready := fmt.Sprintf("%s_%s", d.queuePrefix, pipeline)

	return queueKeys{
		ready:      ready,
		delayed:    ready + ":delayed",
		reserved:   ready + ":reserved",
		dead:       ready + ":dead",
		priorities: ready + ":priorities",
		deliveries: ready + ":deliveries",
		tokens:     ready + ":tokens",
	}
}

func (d *Driver) store(ctx context.Context, item *Item, payload string) (int64, error) {
	keys := d.keys(item.GroupID())
	now := time.Now()
	availableAt := now.Add(item.Options.DelayDuration())

	result, err := storeScript.Run(ctx, d.rdb, []string{
		item.ID(), keys.ready, keys.delayed, keys.reserved, keys.dead,
		keys.priorities, keys.deliveries, keys.tokens,
	}, item.ID(), payload, item.Priority(), availableAt.UnixMilli(), now.UnixMilli()).Int64()
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (d *Driver) reserve(ctx context.Context, pipeline string, limit int64) ([]reservedItem, error) {
	if limit <= 0 {
		return nil, nil
	}

	keys := d.keys(pipeline)
	if err := d.cleanupExpiredDeadLetters(ctx, keys, time.Now()); err != nil {
		return nil, err
	}

	values, err := reserveScript.Run(ctx, d.rdb, []string{
		keys.ready, keys.delayed, keys.reserved, keys.dead,
		keys.priorities, keys.deliveries, keys.tokens,
	}, d.visibilityLimit, d.maxRetries, limit, d.deadLetterTTL).StringSlice()
	if err != nil {
		return nil, err
	}

	items := make([]reservedItem, 0, len(values)/4)
	for index := 0; index+3 < len(values); index += 4 {
		deliveries, conversionError := strconv.Atoi(values[index+3])
		if conversionError != nil {
			return nil, fmt.Errorf("invalid delivery count for task %s: %w", values[index], conversionError)
		}

		items = append(items, reservedItem{
			id:         values[index],
			payload:    values[index+1],
			token:      values[index+2],
			deliveries: deliveries,
		})
	}

	return items, nil
}

func (d *Driver) cleanupExpiredDeadLetters(ctx context.Context, keys queueKeys, now time.Time) error {
	return cleanupExpiredDeadLettersScript.Run(ctx, d.rdb, []string{
		keys.dead, keys.priorities, keys.deliveries, keys.tokens,
	}, now.UnixMilli(), d.deadLetterTTL).Err()
}

func (d *Driver) acknowledge(ctx context.Context, item *Item) (int64, error) {
	keys := d.keys(item.GroupID())

	return acknowledgeScript.Run(ctx, d.rdb, []string{
		item.ID(), keys.ready, keys.delayed, keys.reserved, keys.dead,
		keys.tokens, keys.priorities, keys.deliveries,
	}, item.ID(), item.deliveryToken).Int64()
}

func (d *Driver) requeue(ctx context.Context, item *Item, payload string, delay int) (int64, error) {
	keys := d.keys(item.GroupID())
	now := time.Now()
	availableAt := now.Add(time.Duration(delay) * time.Second)

	return requeueScript.Run(ctx, d.rdb, []string{
		item.ID(), keys.delayed, keys.reserved, keys.dead,
		keys.priorities, keys.deliveries, keys.tokens, keys.ready,
	}, item.ID(), item.deliveryToken, payload, availableAt.UnixMilli(), now.UnixMilli(), d.maxRetries, d.deadLetterTTL).Int64()
}

func (d *Driver) discard(ctx context.Context, item *Item) (int64, error) {
	keys := d.keys(item.GroupID())

	return discardScript.Run(ctx, d.rdb, []string{
		item.ID(), keys.ready, keys.reserved, keys.delayed, keys.dead, keys.tokens,
	}, item.ID(), item.deliveryToken, time.Now().UnixMilli(), d.deadLetterTTL).Int64()
}

func (d *Driver) discardMalformed(ctx context.Context, pipeline string, id string) error {
	keys := d.keys(pipeline)

	return discardMalformedScript.Run(ctx, d.rdb, []string{
		keys.ready, keys.reserved, keys.delayed, keys.dead,
		keys.priorities, keys.deliveries, keys.tokens,
	}, id, time.Now().UnixMilli(), d.deadLetterTTL).Err()
}

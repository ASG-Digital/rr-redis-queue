package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

var _ jobs.Job = (*Item)(nil)

type Item struct {
	Job        string              `json:"job"`
	Ident      string              `json:"id"`
	Payload    []byte              `json:"payload"`
	HeadersMap map[string][]string `json:"headers,omitempty"`
	Options    *Options            `json:"options,omitempty"`

	deliveryToken string
	deliveryCount int
	driver        *Driver
}

type Options struct {
	Priority int64  `json:"priority"`
	Pipeline string `json:"pipeline,omitempty"`
	Delay    int64  `json:"delay,omitempty"`
	AutoAck  bool   `json:"auto_ack"`
	Queue    string `json:"queue,omitempty"`
}

func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

func (i *Item) Body() []byte {
	return i.Payload
}

func (i *Item) Headers() map[string][]string {
	return i.HeadersMap
}

func (i *Item) Context() ([]byte, error) {
	return json.Marshal(struct {
		ID       string              `json:"id"`
		Job      string              `json:"job"`
		Driver   string              `json:"driver"`
		Queue    string              `json:"queue"`
		Headers  map[string][]string `json:"headers"`
		Pipeline string              `json:"pipeline"`
	}{
		ID:       i.Ident,
		Job:      i.Job,
		Driver:   pluginName,
		Queue:    i.Options.Queue,
		Headers:  i.HeadersMap,
		Pipeline: i.Options.Pipeline,
	})
}

func (i *Item) Ack() error {
	if err := i.ensureActive("acknowledge"); err != nil {
		return err
	}

	result, err := i.driver.acknowledge(context.Background(), i)
	if err != nil {
		return errors.E(errors.Str("failed to acknowledge task in Redis"), err)
	}

	if result == transitionResultNoop {
		i.driver.log.Warn("stale acknowledgement ignored", zap.String("taskID", i.ID()))
		return nil
	}

	i.driver.log.Debug("task acknowledged and payload removed", zap.String("taskID", i.ID()))
	return nil
}

func (i *Item) Nack() error {
	if err := i.ensureActive("negatively acknowledge"); err != nil {
		return err
	}

	// RoadRunner uses Nack for worker allocation and protocol failures. Retry
	// transient infrastructure failures without allowing a poison task to loop.
	i.driver.log.Warn("task negatively acknowledged by RoadRunner; scheduling bounded retry",
		zap.String("taskID", i.ID()),
		zap.Int("delivery", i.deliveryCount),
	)

	return i.requeue(nil, i.driver.retryDelay)
}

func (i *Item) NackWithOptions(requeue bool, delay int) error {
	if err := i.ensureActive("negatively acknowledge"); err != nil {
		return err
	}

	if requeue {
		return i.requeue(nil, delay)
	}

	result, err := i.driver.discard(context.Background(), i)
	if err != nil {
		return errors.E(errors.Str("failed to discard task in Redis"), err)
	}
	if result == transitionResultNoop {
		i.driver.log.Warn("stale task discard ignored", zap.String("taskID", i.ID()))
	}

	return nil
}

func (i *Item) Requeue(headers map[string][]string, delay int) error {
	if err := i.ensureActive("requeue"); err != nil {
		return err
	}

	return i.requeue(headers, delay)
}

func (i *Item) requeue(headers map[string][]string, delay int) error {
	if i.HeadersMap == nil {
		i.HeadersMap = make(map[string][]string)
	}

	if len(headers) > 0 {
		maps.Copy(i.HeadersMap, headers)
	}

	if delay < 0 {
		delay = 0
	}
	i.Options.Delay = int64(delay)

	return i.driver.handleRequeue(context.Background(), i, delay)
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	headers := job.Headers()
	if headers == nil {
		headers = make(map[string][]string)
	}

	return &Item{
		Job:        job.Name(),
		Ident:      job.ID(),
		Payload:    job.Payload(),
		HeadersMap: headers,
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}

func (i *Item) ensureActive(operation string) error {
	if i.Options == nil {
		return fmt.Errorf("cannot %s task %s: options are nil", operation, i.Ident)
	}
	if i.driver == nil {
		return fmt.Errorf("cannot %s task %s: driver is nil", operation, i.Ident)
	}
	if atomic.LoadUint64(&i.driver.stopped) == 1 {
		return fmt.Errorf("cannot %s task %s: pipeline is stopped", operation, i.Ident)
	}

	return nil
}

func (d *Driver) addItemRuntime(item *Item, reserved reservedItem) {
	item.driver = d
	item.deliveryToken = reserved.token
	item.deliveryCount = reserved.deliveries
}

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
	"maps"
	"sync/atomic"
	"time"
)

var _ jobs.Job = (*Item)(nil)

type Item struct {
	Job        string              `json:"job"`
	Ident      string              `json:"id"`
	Payload    []byte              `json:"payload"`
	HeadersMap map[string][]string `json:"headers,omitempty"`
	Options    *Options            `json:"options,omitempty"`
}

type Options struct {
	Priority  int64  `json:"priority"`
	Pipeline  string `json:"pipeline,omitempty"`
	Delay     int64  `json:"delay,omitempty"`
	AutoAck   bool   `json:"auto_ack"`
	Queue     string `json:"queue,omitempty"`
	requeueFn func(ctx context.Context, item *Item) error
	stopped   *uint64
	rdb       redis.UniversalClient
	ctx       context.Context
	log       *zap.Logger
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

	if i.Options == nil {
		return fmt.Errorf("ack failed: options are nil for item %s", i.Ident)
	}
	if i.Options.rdb == nil {
		return fmt.Errorf("ack failed: Redis client is nil for item %s", i.Ident)
	}

	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the job, the pipeline is probably stopped")
	}
	if i.Options.AutoAck {
		return nil
	}

	err := i.Options.rdb.Del(i.Options.ctx, i.ID()).Err()
	if err != nil {
		return errors.E(errors.Str("failed to remove payload from Redis"), err)
	}

	i.Options.log.Warn("Task acknowledged and payload removed", zap.String("taskID", i.ID()))
	return nil

}

func (i *Item) Nack() error {
	i.Options.log.Debug("Task negatively acknowledged with", zap.String("taskID", i.ID()))
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to negatively acknowledge the job, the pipeline is probably stopped")
	}
	return nil
}

func (i *Item) NackWithOptions(requeue bool, _ int) error {
	i.Options.log.Debug("Task negatively acknowledged (with options) with", zap.String("taskID", i.ID()))
	if requeue {
		return i.Requeue(nil, 0)
	}
	return i.Nack()
}

func (i *Item) Requeue(headers map[string][]string, _ int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to requeue the job, the pipeline is probably stopped")
	}

	if i.HeadersMap == nil {
		i.HeadersMap = make(map[string][]string)
	}

	if len(headers) > 0 {
		maps.Copy(i.HeadersMap, headers)
	}

	if i.Options.requeueFn != nil {
		return i.Options.requeueFn(context.Background(), i)
	}
	return nil
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

func (d *Driver) addItemOptions(
	item *Item,
) {
	item.Options.rdb = d.rdb
	item.Options.ctx = d.ctx
	item.Options.log = d.log
	item.Options.requeueFn = d.handleRequeue
	item.Options.stopped = &d.stopped
}

package queue

import (
	"context"
	"encoding/json"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
	"sync/atomic"
	"time"
)

var _ jobs.Job = (*Item)(nil)

type Item struct {
	Job     string `json:"job"`
	Ident   string `json:"id"`
	Payload []byte `json:"payload"`
	headers map[string][]string
	Options *Options
}

type Options struct {
	Priority   int64  `json:"priority"`
	Pipeline   string `json:"pipeline,omitempty"`
	Delay      int64  `json:"delay,omitempty"`
	AutoAck    bool   `json:"auto_ack"`
	Queue      string `json:"queue,omitempty"`
	RequeueFn  func(ctx context.Context, item *Item) error
	stopped    *uint64
	rawPayload string
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
	return i.Headers()
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
		Headers:  i.headers,
		Pipeline: i.Options.Pipeline,
	})
}

func (i *Item) Ack() error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the job, the pipeline is probably stopped")
	}
	if i.Options.AutoAck {
		return nil
	}
	return nil
}

func (i *Item) Nack() error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to negatively acknowledge the job, the pipeline is probably stopped")
	}
	return nil
}

func (i *Item) NackWithOptions(requeue bool, _ int) error {
	if requeue {
		return i.Requeue(nil, 0)
	}
	return i.Nack()
}

func (i *Item) Requeue(headers map[string][]string, _ int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to requeue the job, the pipeline is probably stopped")
	}

	if i.headers == nil {
		i.headers = make(map[string][]string)
	}

	for key, value := range headers {
		i.headers[key] = value
	}

	if i.Options.RequeueFn != nil {
		return i.Options.RequeueFn(context.Background(), i)
	}
	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}

func (d *Driver) unpack(
	taskID, payload, headersJSON string,
	priority float64,
	pipeline *atomic.Pointer[jobs.Pipeline],
	requeueFn func(ctx context.Context, item *Item) error,
	stopped *uint64,
) *Item {
	headers := parseHeaders(headersJSON)
	return &Item{
		Ident:   taskID,
		Payload: []byte(payload),
		headers: headers,
		Options: &Options{
			Priority: int64(priority),
			Pipeline: (*pipeline.Load()).Name(),
			stopped:  stopped,
			RequeueFn: func(ctx context.Context, item *Item) error {
				return requeueFn(ctx, item)
			},
		},
	}
}

func parseHeaders(headersJSON string) map[string][]string {
	headers := make(map[string][]string)
	_ = json.Unmarshal([]byte(headersJSON), &headers)
	return headers
}

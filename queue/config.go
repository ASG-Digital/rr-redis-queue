package queue

import "time"

const (
	channelKey           string = "channel"
	visibilityTimeoutKey string = "visibility_timeout"
	prefetchLimitKey     string = "prefetch_limit"
	maxRetriesKey        string = "max_retries"
	retryDelayKey        string = "retry_delay"
	deadLetterTTLKey     string = "dead_letter_ttl"
	queuePrefixKey       string = "prefix"
	tickerIntervalKey    string = "ticker_interval"
)

type Config struct {
	Connection      string        `mapstructure:"connection"`
	Channel         string        `mapstructure:"channel"`
	PrefetchLimit   int           `mapstructure:"prefetch_limit"`
	VisibilityLimit int           `mapstructure:"visibility_timeout"`
	MaxRetries      int           `mapstructure:"max_retries"`
	RetryDelay      int           `mapstructure:"retry_delay"`
	DeadLetterTTL   int           `mapstructure:"dead_letter_ttl"`
	QueuePrefix     string        `mapstructure:"prefix"`
	TickerInterval  time.Duration `mapstructure:"ticker_interval"`
}

func (c *Config) InitDefaults() {
	if c.PrefetchLimit == 0 {
		c.PrefetchLimit = 10
	}

	if c.VisibilityLimit == 0 {
		c.VisibilityLimit = 30
	}

	if c.MaxRetries == 0 {
		c.MaxRetries = 1
	}

	if c.RetryDelay == 0 {
		c.RetryDelay = 5
	}

	if c.DeadLetterTTL == 0 {
		c.DeadLetterTTL = 7 * 24 * 60 * 60
	}

	if c.Channel == "" {
		c.Channel = "queue_channel"
	}

	if c.TickerInterval <= 0 {
		c.TickerInterval = time.Minute
	}
}

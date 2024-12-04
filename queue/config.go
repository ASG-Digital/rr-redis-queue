package queue

const (
	channelKey           string = "channel"
	visibilityTimeoutKey string = "visibility_timeout"
	prefetchLimitKey     string = "prefetch_limit"
	maxRetriesKey        string = "max_retries"
	queuePrefixKey       string = "prefix"
	priorityKey          string = "priority"
)

type Config struct {
	Connection      string `mapstructure:"connection"`
	Channel         string `mapstructure:"channel"`
	PrefetchLimit   int    `mapstructure:"prefetch_limit"`
	VisibilityLimit int    `mapstructure:"visibility_timout"`
	MaxRetries      int    `mapstructure:"max_retries"`
	QueuePrefix     string `mapstructure:"prefix"`
	Priority        int    `mapstructure:"priority"`
}

func (c *Config) InitDefaults() {
	if c.PrefetchLimit == 0 {
		c.PrefetchLimit = 10
	}

	if c.VisibilityLimit == 0 {
		c.VisibilityLimit = 30
	}

	if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}

	if c.Priority == 0 {
		c.Priority = 1
	}

	if c.Channel == "" {
		c.Channel = "queue_channel"
	}
}

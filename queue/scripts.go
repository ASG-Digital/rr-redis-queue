package queue

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

// The scripts are embedded into the plugin binary at build time. Keeping each
// atomic Redis transition in its own file makes the queue state machine easier
// to review without introducing runtime file-system dependencies.

//go:embed scripts/store.lua
var storeLua string

//go:embed scripts/reserve.lua
var reserveLua string

//go:embed scripts/acknowledge.lua
var acknowledgeLua string

//go:embed scripts/requeue.lua
var requeueLua string

//go:embed scripts/discard.lua
var discardLua string

//go:embed scripts/discard_malformed.lua
var discardMalformedLua string

//go:embed scripts/cleanup_expired_dead_letters.lua
var cleanupExpiredDeadLettersLua string

var (
	storeScript                     = redis.NewScript(storeLua)
	reserveScript                   = redis.NewScript(reserveLua)
	acknowledgeScript               = redis.NewScript(acknowledgeLua)
	requeueScript                   = redis.NewScript(requeueLua)
	discardScript                   = redis.NewScript(discardLua)
	discardMalformedScript          = redis.NewScript(discardMalformedLua)
	cleanupExpiredDeadLettersScript = redis.NewScript(cleanupExpiredDeadLettersLua)
)

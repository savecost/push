package broker

import (
	"time"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond
	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 1 * time.Second

	WRITE_DEADLINE = 2 * time.Second

	MAX_MESSAGE_BATCH      = 200
	MAX_MESSAGE_PULL_COUNT = 200

	MAX_CHANNEL_LEN = 1000
)

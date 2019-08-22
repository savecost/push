package message

const (
	PUB_BATCH     = 'a'
	PUB_ONE       = 'b'
	PUB_TIMER     = 'c'
	PUB_TIMER_ACK = 'd'
	PUB_RESTORE   = 'e'

	SUB    = 'f'
	SUBACK = 'g'
	UNSUB  = 'h'

	PING = 'i'
	PONG = 'j'

	COUNT = 'k'
	PULL  = 'l'

	CONNECT    = 'm'
	CONNECT_OK = 'n'

	BROADCAST = 'o'

	REDUCE_COUNT = 'p'
	MARK_READ    = 'r'

	JOIN_CHAT        = 's'
	LEAVE_CHAT       = 't'
	PRESENCE_ONLINE  = 'u'
	PRESENCE_OFFLINE = 'v'
	PRESENCE_ALL     = 'q'
	ALL_CHAT_USERS   = 'w'

	RETRIEVE = 'x'
)

const (
	NORMAL_MSG = 0
	TIMER_MSG  = 1
)

const (
	QOS0 = 0
	QOS1 = 1
)

var (
	DEFAULT_QUEUE     = []byte("meq.io")
	MSG_NEWEST_OFFSET = []byte("0")
)

const (
	MAX_PULL_COUNT = 100

	CacheFlushLen = 200

	REDUCE_ALL_COUNT = 0

	MAX_IDLE_TIME = 120

	NeverExpires = 0

	MSG_ID_LENGTH = 19
)

package message

type Pub struct {
	RawID     []byte
	ID        []byte
	Topic     []byte
	Payload   []byte
	Acked     bool
	Type      int8
	QoS       int8
	TTL       int64
	Sender    []byte
	Timestamp []byte
}

type Ack struct {
	Topic []byte
	Msgid []byte
}

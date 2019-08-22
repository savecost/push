package broker

import (
	"context"
	"net"
	"sync"
	"time"

	"net/http"

	"github.com/imdevlab/g"
	"github.com/vgoio/vgo/internal/pkg/cluster"
	"github.com/vgoio/vgo/internal/pkg/config"
	"github.com/vgoio/vgo/internal/pkg/network/listener"
	"github.com/vgoio/vgo/internal/pkg/network/websocket"

	"go.uber.org/zap"

	"github.com/kelindar/tcp"
)

type Broker struct {
	context context.Context
	cancel  context.CancelFunc

	http *http.Server
	tcp  *tcp.Server

	wg      *sync.WaitGroup
	running bool

	clients map[uint64]*client

	cluster *cluster.Cluster

	gid uint64

	sync.RWMutex
}

func New(path string) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		context: ctx,
		cancel:  cancel,
		http:    new(http.Server),
		tcp:     new(tcp.Server),

		wg:      &sync.WaitGroup{},
		clients: make(map[uint64]*client),
	}

	// init base config
	config.Init(path)

	mux := http.NewServeMux()
	// mux.HandleFunc("/keygen", s.onHTTPKeyGen)
	mux.HandleFunc("/", b.onRequest)
	b.http.Handler = mux
	b.tcp.OnAccept = func(conn net.Conn) {
		c := newClient(conn, b)
		go c.process()
	}

	g.InitLogger(config.Conf.Common.LogLevel)
	g.L.Info("base configuration loaded")

	return b
}

func (b *Broker) Start() {
	b.gid = config.Conf.Broker.ID * 1e11
	b.running = true

	b.cluster = cluster.New()

	go b.listen()

	// init store
	switch config.Conf.Storage.Provider {
	case "memory":

	case "fdb":

	}
}

func (b *Broker) Shutdown() {
	b.running = false

	for _, c := range b.clients {
		c.conn.Close()
	}

	g.L.Sync()
	b.wg.Wait()
}

func (b *Broker) onRequest(w http.ResponseWriter, r *http.Request) {
	if conn, ok := websocket.TryUpgrade(w, r); ok {
		c := newClient(conn, b)
		go c.process()
	}
}

// Listen starts the service.
func (b *Broker) listen() {
	l, err := listener.New(config.Conf.Broker.Listen, nil)
	if err != nil {
		g.L.Fatal("create listener error", zap.Error(err))
	}

	// Set the read timeout on our mux listener
	l.SetReadTimeout(120 * time.Second)

	// Configure the matchers
	l.ServeAsync(listener.MatchHTTP(), b.http.Serve)
	l.ServeAsync(listener.MatchAny(), b.tcp.Serve)
	err = l.Serve()
	if err != nil {
		g.L.Fatal("listen error", zap.Error(err))
	}
}

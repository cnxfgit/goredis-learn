package handler

import (
	"context"
	"goredis/log"
	"goredis/server"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type Handler struct {
	sync.Once
	mu     sync.RWMutex
	conns  map[net.Conn]struct{}
	closed atomic.Bool

	db        DB
	parser    Parser
	persister Persister
	logger    log.Logger
}

func NewHandler(db DB, persister Persister, parser Parser, logger log.Logger) (server.Handler, error) {
	h := Handler{
		conns:     make(map[net.Conn]struct{}),
		persister: persister,
		logger:    logger,
		db:        db,
		parser:    parser,
	}

	return &h, nil
}

func (h *Handler) Start() error {
	// 加载持久化文件
	reloader, err := h.persister.Reloader()
	if err != nil {
		return err
	}
	defer reloader.Close()

	// 读取持久化文件内容，还原内存数据库
	h.handle(SetLoadingPattern(context.Background()), newFakeReaderWriter(reloader))
	return nil
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	h.mu.Lock()
	if h.closed.Load() {
		h.mu.Unlock()
		return
	}

	h.conns[conn] = struct{}{}
	h.mu.Unlock()

	h.handle(ctx, conn)
}

func (h *Handler) handle(ctx context.Context, conn io.ReadWriter) {
	// 借助 protocol parser 将到来的指令转而通过 stream channel 输出
	stream := h.parser.ParseStream(conn)
	for {
		select {
		case <- ctx.Done():
			h.logger.Warnf("[handler]handle ctx err: %s", ctx.Err().Error())
			return
		case droplet := <-stream:
			if err := h.handleDroplet(ctx, conn, droplet); err != nil {
				h.logger.Errorf("[handler]conn terminated, err: %s", droplet.Err.Error())
				return
			}
		}
	}
}

func (h *Handler) handleDroplet(ctx context.Context, conn io.ReadWriter, droplet *Droplet) error {
	if droplet.Terminated() {
		return droplet.Err
	}

	if droplet.Reply == nil {
		h.logger.Errorf("[handler]conn empty request")
		return nil
	}

	multiReply, ok := droplet.Reply.(MultiReply)
	if !ok {
		h.logger.Errorf("[handler]conn invalid request: %s", droplet.Reply.ToBytes())
		return nil
	}

	if reply := h.db.Do(ctx, multiReply.Args()); reply != nil {
		_, _ = conn.Write(reply.ToBytes())
		return nil
	}

	_, _ = conn.Write(UnknownErrReplyBytes)
	return nil
}

func (h *Handler) Close() {
	h.Once.Do(func() {
		h.closed.Store(true)
		h.mu.RLock()
		defer h.mu.RUnlock()

		for conn := range h.conns {
			if err := conn.Close; err != nil {

			}
		}
		h.conns = nil
		h.db.Close()
		h.persister.Close()
	})
}

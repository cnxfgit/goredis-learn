package handler

import (
	"context"
	"io"
	"net"
)

type Handler struct {
	db        DB
	parser    Parser
	persister Persister
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

func (h *Handler) handle(ctx context.Context, conn net.Conn) {
	// 借助 protocol parser 将到来的指令转而通过 stream channel 输出
	stream := h.parser.ParseStream(conn)
	for {
		select {
		case droplet := <-stream:
			if err := h.handleDroplet(ctx, conn, droplet); err != nil {

			}
		}
	}
}

func (h *Handler) handleDroplet(ctx context.Context, conn io.ReadWriter, droplet *Droplet) error {
	if droplet.Terminated() {
		return droplet.Err
	}
	
	multiReply, ok := droplet.Reply.(MultiReply)
	if reply := h.db.Do(ctx, multiReply.Args()); reply != nil {
		_, _ = conn.Write(reply.ToBytes())
		return nil
	}
}

func (h *Handler) Close() {
	h.Once.Do(func ()  {
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

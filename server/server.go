package server

import (
	"context"
	"goredis/pool"
	"net"
)

// 逻辑处理器
type Handler interface {
	Start() error
	Handle(ctx context.Context, conn net.Conn)
	Close()
}

type Server struct {
	handler Handler
}

func (s *Server) Serve(address string) error {
	// 启动handler
	s.handler.Start()

	// 运行tcp服务
	s.listenAndServe(listener, closec)
}

func (s *Server) listenAndServe(listener net.Listener, closec chan struct{}) {
	// io多路复用模型
	for {
		// 接收tcp连接
		conn, err := listener.Accept()

		// 分配goroutine处理
		pool.Submit(func ()  {
			s.handler.Handle(ctx, conn)
		})
	}
}

func (s *Server) Stop()  {
	s.stopOnce.Do(func ()  {
		close(s.stopc)
	})
}

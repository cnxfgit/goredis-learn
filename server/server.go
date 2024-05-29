package server

import (
	"context"
	"goredis/pool"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// 处理器
type Handler interface {
	Start() error // 启动 handler
	// 处理到来的每一笔 tcp 连接
	Handle(ctx context.Context, conn net.Conn)
	// 关闭处理器
	Close()
}

type Server struct {
	runOnce  sync.Once
	stopOnce sync.Once
	handler  Handler
	logger   log.Logger
	stopc    chan struct{}
}

func (s *Server) Serve(address string) error {
	if err := s.handler.Start(); err != nil {
		return err
	}

	var _err error
	s.runOnce.Do(func() {
		exitWords := []os.Signal{syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, exitWords...)
		closec := make(chan struct{}, 4)
		pool.Submit(func() {
			for {
				select {
				case signal := <-sigc:
					switch signal {
					case exitWords[0], exitWords[1], exitWords[2], exitWords[3]:
						closec <- struct{}{}
						return
					default:
					}
				case <-s.stopc:
					closec <- struct{}{}
					return
				}
			}
		})

		listener, err := net.Listen("tcp", address)
		if err != nil {
			_err = err
			return
		}

		s.listenAndServe(listener, closec)
	})

	return _err
}

func (s *Server) listenAndServe(listener net.Listener, closec chan struct{}) {
	errc := make(chan error, 1)
	defer close(errc)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Submit(func() {
		select {
		case <-closec:
		case err := <-errc:
		}
		cancel()

		s.handler.Close()
		if err := listener.Close(); err != nil {
			s.logger.Errorf("[server]server close listener err: %s", err.Error())
		}
	})

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			errc <- err
			break
		}

		wg.Add(1)
		pool.Submit(func() {
			defer wg.Done()
			s.handler.Handle(ctx, conn)
		})
	}

	wg.Wait()
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopc)
	})
}

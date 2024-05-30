package persist

import (
	"context"
	"goredis/handler"
	"goredis/lib/pool"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type appendSyncStrategy string

func (a appendSyncStrategy) string() string {
	return string(a)
}

const (
	alwaysAppendSyncStrategy   appendSyncStrategy = "always"   // 每条指令都进行持久化落盘
	everysecAppendSyncStrategy appendSyncStrategy = "everysec" // 每秒批量执行一次持久化落盘
	noAppendSyncStrategy       appendSyncStrategy = "no"       // 不主动进行指令的持久化落盘，由设备自行决定落盘节奏
)

type aofPersister struct {
	ctx    context.Context
	cancel context.CancelFunc

	buffer                 chan [][]byte
	aofFile                *os.File
	aofFileName            string
	appendFsync            appendSyncStrategy
	autoAofRewriteAfterCmd int64
	aofCounter             atomic.Int64

	mu   sync.Mutex
	once sync.Once
}

func newAofPersister(thinker Thinker) (handler.Persister, error) {
	aofFileName := thinker.AppendFileName()
	aofFile, err := os.OpenFile(aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := aofPersister{
		ctx:         ctx,
		cancel:      cancel,
		buffer:      make(chan [][]byte, 1<<10),
		aofFile:     aofFile,
		aofFileName: aofFileName,
	}

	if autoAofRewriteAfterCmd := thinker.AutoAofRewriteAfterCmd(); autoAofRewriteAfterCmd > 1 {
		a.autoAofRewriteAfterCmd = int64(autoAofRewriteAfterCmd)
	}

	switch thinker.AppendFsync() {
	case alwaysAppendSyncStrategy.string():
		a.appendFsync = alwaysAppendSyncStrategy
	case everysecAppendSyncStrategy.string():
		a.appendFsync = everysecAppendSyncStrategy
	default:
		a.appendFsync = noAppendSyncStrategy // 默认策略
	}

	pool.Submit(a.run)
	return &a, nil
}

func (a *aofPersister) Reloader() (io.ReadCloser, error) {
	file, err := os.Open(a.aofFileName)
	if err != nil {
		return nil, err
	}
	_, _ = file.Seek(0, io.SeekStart)
	return file, nil
}

func (a *aofPersister) PersistCmd(ctx context.Context, cmd [][]byte) {
	if handler.IsLoadingPattern(ctx) {
		return
	}
	a.buffer <- cmd
}

func (a *aofPersister) Close() {
	a.once.Do(func() {
		a.cancel()
		_ = a.aofFile.Close()
	})
}

func (a *aofPersister) run() {
	if a.appendFsync == everysecAppendSyncStrategy {
		pool.Submit(a.fsyncEverySecond)
	}

	for {
		select {
        case <- a.ctx.Done():
            return
		case cmd := <-a.buffer:
			a.writeAof(cmd)
			a.aofTick()
		}
	}
}

func (a *aofPersister) aofTick() {
	// 如果阈值 <= 1，代表不启用 aof 指令重写策略
	if a.autoAofRewriteAfterCmd <= 1 {
		return
	}

	// 累加指令执行计数器
	if ticked := a.aofCounter.Add(1); ticked < int64(a.autoAofRewriteAfterCmd) {
		return
	}

	// 执行 aof 指令持久化次数达到重写阈值，进行重写，并将计数器清零
	_ = a.aofCounter.Add(-a.autoAofRewriteAfterCmd)
	pool.Submit(func() {
		if err := a.rewriteAOF(); err != nil {
			// log
		}
	})
}

func (a *aofPersister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
        case <- a.ctx.Done():
            return
		case <-ticker.C:
			if err := a.fsync(); err != nil {
				// log
			}
		}
	}
}


func (a *aofPersister) writeAof(cmd [][]byte) {
	// 1 加锁保证并发安全
	a.mu.Lock()
	defer a.mu.Unlock()

	// 2 将指令封装为 multi bulk reply 形式
	persistCmd := handler.NewMultiBulkReply(cmd)
	// 3 指令 append 写入到 aof 文件
	if _, err := a.aofFile.Write(persistCmd.ToBytes()); err != nil {
		// log
		return
	}

	// 4 除非持久化策略等级为 always，否则不需要立即执行 fsync 操作，强制进行指令落盘(性能较差)
	if a.appendFsync != alwaysAppendSyncStrategy {
		return
	}

	// 5 fsync 操作，指令强制落盘
	if err := a.fsyncLocked(); err != nil {
		// log
	}
}

func (a *aofPersister) fsync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.fsyncLocked()
}

func (a *aofPersister) fsyncLocked() error {
	return a.aofFile.Sync()
}
package persist

import (
	"context"
	"goredis/handler"
	"goredis/lib"
	"goredis/log"
	"goredis/lib/pool"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type appendSyncStrategy string

const (
	alwaysAppendSyncStrategy   appendSyncStrategy = "always"   // 每条指令都进行持久化落盘
	everysecAppendSyncStrategy appendSyncStrategy = "everysec" // 每秒批量执行一次持久化落盘
	noAppendSyncStrategy       appendSyncStrategy = "no"       // 不主动进行指令的持久化落盘，由设备自行决定落盘节奏
)

type aofPersister struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	buffer                 chan [][]byte
	aofFile                *os.File
	aofFileName            string
	appendFsync            appendSyncStrategy
	autoAofRewriteAfterCmd int64
	aofCounter             atomic.Int64
	mu                     sync.Mutex
	once                   sync.Once
}

func NewPersister(thinker Thinker) (handler.Persister, error) {
	if !thinker.AppendOnly() {
		return newFakePersister(nil), nil
	}

	return newAofPersister(thinker)
}

func (a *aofPersister) run() {
	if a.appendFsync == everysecAppendSyncStrategy {
		pool.Submit(a.fsyncEverySecond)
	}

	for {
		select {
		case cmd := <- a.buffer:
			a.writeAof(cmd)
			a.aofTick()
		}
	}
}

func (a *aofPersister) fsyncEverySecond() {
    ticker := time.NewTicker(time.Second)
    for {
        select {
        // ...
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

func (a *aofPersister) Reloader() (io.ReadCloser, error) {
    file, err := os.Open(a.aofFileName)
    if err != nil {
        return nil, err
    }
    _, _ = file.Seek(0, io.SeekStart)
    return file, nil
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

func (a *aofPersister) rewriteAOF() error {
    // 1 重写前处理. 需要短暂加锁
    tmpFile, fileSize, err := a.startRewrite()
    if err != nil {
        return err
    }


    // 2 aof 指令重写. 与主流程并发执行
    if err = a.doRewrite(tmpFile, fileSize); err != nil {
        return err
    }


    // 3 完成重写. 需要短暂加锁
    return a.endRewrite(tmpFile, fileSize)
}

func (a *aofPersister) startRewrite() (*os.File, int64, error) {
    a.mu.Lock()
    defer a.mu.Unlock()


    if err := a.aofFile.Sync(); err != nil {
        return nil, 0, err
    }


    fileInfo, _ := os.Stat(a.aofFileName)
    fileSize := fileInfo.Size()


    // 创建一个临时的 aof 文件
    tmpFile, err := os.CreateTemp("./", "*.aof")
    if err != nil {
        return nil, 0, err
    }


    return tmpFile, fileSize, nil
}

func (a *aofPersister) doRewrite(tmpFile *os.File, fileSize int64) error {
    forkedDB, err := a.forkDB(fileSize)
    if err != nil {
        return err
    }


    // 将 db 数据转为 aof cmd
    forkedDB.ForEach(func(key string, adapter database.CmdAdapter, expireAt *time.Time) {
        _, _ = tmpFile.Write(handler.NewMultiBulkReply(adapter.ToCmd()).ToBytes())


        if expireAt == nil {
            return
        }


        expireCmd := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(*expireAt))}
        _, _ = tmpFile.Write(handler.NewMultiBulkReply(expireCmd).ToBytes())
    })


    return nil
}

func (a *aofPersister) forkDB(fileSize int64) (database.DataStore, error) {
    file, err := os.Open(a.aofFileName)
    if err != nil {
        return nil, err
    }
    file.Seek(0, io.SeekStart)
    logger := log.GetDefaultLogger()
    reloader := readCloserAdapter(io.LimitReader(file, fileSize), file.Close)
    fakePerisister := newFakePersister(reloader)
    tmpKVStore := datastore.NewKVStore(fakePerisister)
    executor := database.NewDBExecutor(tmpKVStore)
    trigger := database.NewDBTrigger(executor)
    h, err := handler.NewHandler(trigger, fakePerisister, protocol.NewParser(logger), logger)
    if err != nil {
        return nil, err
    }
    if err = h.Start(); err != nil {
        return nil, err
    }
    return tmpKVStore, nil
}

func (a *aofPersister) endRewrite(tmpFile *os.File, fileSize int64) error {
    a.mu.Lock()
    defer a.mu.Unlock()


    // copy commands executed during rewriting to tmpFile
    /* read write commands executed during rewriting */
    src, err := os.Open(a.aofFileName)
    if err != nil {
        return err
    }
    defer func() {
        _ = src.Close()
        _ = tmpFile.Close()
    }()


    if _, err = src.Seek(fileSize, 0); err != nil {
        return err
    }


    // 把老的 aof 文件中后续内容 copy 到 tmp 中
    if _, err = io.Copy(tmpFile, src); err != nil {
        return err
    }


    // 关闭老的 aof 文件，准备废弃
    _ = a.aofFile.Close()
    // 重命名 tmp 文件，作为新的 aof 文件
    if err := os.Rename(tmpFile.Name(), a.aofFileName); err != nil {
        // log
    }


    // 重新开启
    aofFile, err := os.OpenFile(a.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
    if err != nil {
        panic(err)
    }
    a.aofFile = aofFile
    return nil
}

type Thinker interface {
	AppendOnly() bool
	AppendFileName() string
	AppendFsync() string
	AutoAofRewriteAfterCmd() int
}

func newAofPersister(thinker Thinker) (handler.Persister, error) {
	// aof 文件名称
	aofFileName := thinker.AppendFileName()
	// 打开 aof 文件
	aofFile, err := os.OpenFile(aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)

	// 构造持久化模块实例
	a := aofPersister{
		ctx:         ctx,
		cancel:      cancel,
		buffer:      make(chan [][]byte, 1<<10),
		aofFile:     aofFile,
		aofFileName: aofFileName,
	}

	// 判断是否启用了 aof 指令重写策略
	if autoAofRewriteAfterCmd := thinker.AutoAofRewriteAfterCmd(); autoAofRewriteAfterCmd > 1 {
		a.autoAofRewriteAfterCmd = int64(autoAofRewriteAfterCmd)
	}

	// 设置 aof 持久化策略级别
	switch thinker.AppendFsync() {
	case alwaysAppendSyncStrategy.string():
		a.appendFsync = alwaysAppendSyncStrategy
	case everysecAppendSyncStrategy.string():
		a.appendFsync = everysecAppendSyncStrategy
	default:
		a.appendFsync = noAppendSyncStrategy // 默认策略
	}

	// 启动持久化模块常驻运行协程
	pool.Submit(a.run)
	return &a, nil
}

package database

import (
	"context"
	"time"
)

type DBExecutor struct {
	ctx         context.Context
	cancel      context.CancelFunc
	ch          chan *Command
	cmdHandlers map[CmdType]CmdHandler
	dataStore   DataStore
	gcTicker    *time.Ticker
}

func (e *DBExecutor) Entrance() chan<- *Command {
	return e.ch
}

func (e *DBExecutor) run() {
	for {
		select {
		case <-e.gcTicker.C:
			e.dataStore.GC()
		case cmd := <-e.ch:
			cmdFunc, ok := e.cmdHandlers[cmd.cmd]
			e.dataStore.ExpirePreprocess(string(cmd.args[0]))
			cmd.receiver <- cmdFunc(cmd)
		}
	}
}

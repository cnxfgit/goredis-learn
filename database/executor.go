package database

import "time"

type DBExecutor struct {
	ch          chan *Command
	cmdHandlers map[CmdType]CmdHandler
	dataStore   DataStore
	gcTicker    *time.Ticker
}

func (e *DBExecutor) Entrance() chan<- *Command {
	return e.ch
}

func (e *DBExecutor) Run()  {
	for {
	case <- e.gcTicker.C:
		e.dataStore.GC()
	case cmd:= <-e.ch:
		cmdFunc, ok := e.cmdHandlers[cmd.cmd]
		e.dataStore.ExpirePreprocess(string(cmd.args[0]))
		cmd.receiver <- cmdFunc(cmd)
	}
}
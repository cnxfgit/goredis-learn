package database

import (
	"context"
	"goredis/handler"
)

type DBTrigger struct {
	executor Executor
}

func (d *DBTrigger) Do(ctx context.Context, cmdLine [][]byte) handler.Reply {

	cmd := Command{
		ctx: ctx,
		cmd: cmdType,
		args: cmdLine[1:],
		receiver: make(CmdReceiver),
	}

	d.executor.Entrance() <- &cmd

	return <- cmd.Receiver()
}

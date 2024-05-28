package handler

import (
	"bufio"
	"bytes"
	"context"
	"goredis/handler"
	"goredis/pool"
	"io"
)

type Parser struct {
	lineParser map[byte]lineParser
}

func (p *Parser) ParseStream(reader io.Reader) <-chan *handler.Droplet {
	ch := make(chan *handler.Droplet)
	pool.Submit(func() {
		p.parse(reader, ch)
	})
	return ch
}

func (p *Parser) parse(rawReader io.Reader, ch chan<-*handler.Droplet) {
	reader := bufio.NewReader(rawReader)
	for {
		firstLine, err := reader.ReadBytes('\n')

		length := len(firstLine)
		if length <= 2|| firstLine[length-1] != '\n' || firstLine[length-2] != '\r' {
			continue
		}

		firstLine = bytes.TrimSuffix(firstLine, []byte{'\r', '\n'})
		lineParseFunc, ok := p.lineParsers[firstLine[0]]
		if !ok {
			p.logger.Errorf("[parser] invalid line handler: %s", firstLine[0])
			continue
		}

		ch <- lineParseFunc(firstLine, reader)
	}
}

type DB interface {
	Do(ctx context.Context, cmdLine [][]byte) Reply
	Close()
}


type Executor interface {
	Entrance() chan <- *Command
	ValidCommand(cmd CmdType) bool
	Close()
}
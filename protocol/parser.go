package protocol

import (
	"goredis/handler"
	"log"
)

type Parser struct {
	lineParsers map[byte]lineParser
}

func NewParser(logger log.Logger) handler.Parser {
	p:= Parser{

	}

	p.lineParsers = map[byte]lineParser{
		'+': p.parseSimpleString,
		'-': p.parseError,
		':': p.parseInt,
		'$': p.parseBulk,
		'*': p.parseMultoBulk,
	}

	return p
}

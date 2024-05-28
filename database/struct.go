package database

import "goredis/handler"

type DBStore interface {
	GC()
	Expire(*Command) handler.Reply
	ExpireAt(*Command) handler.Reply

	Get(*Command) handler.Reply
	Set(*Command) handler.Reply

	LPush(*Command) handler.Reply
	SAdd(*Command) handler.Reply
	HSet(*Command) handler.Reply
	ZAdd(*Command) handler.Reply
}

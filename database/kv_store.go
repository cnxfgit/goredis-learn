package database

import (
	"goredis/handler"
	"time"
)

type KVStore struct {
	data map[string]interface{}
	expireAt map[string]time.Time

	expireTimeWheel SortedSet

	persister handler.Persister
}
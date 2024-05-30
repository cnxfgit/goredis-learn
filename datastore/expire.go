package datastore

import (
	"goredis/lib"
	"time"
)

func (k *KVStore) GC() {
	nowUnix := lib.TimeNow().Unix()
	for _, expiredKey := range k.expireTimeWheel.Range(0, nowUnix) {
		k.expireProcess(expiredKey)
	}
}

func (k *KVStore) ExpirePreprocess(key string)  {
	expiredAt, ok := k.expiredAt[key]
	if !ok {
		return
	}

	if expiredAt.After(lib.TimeNow()) {
		return
	}

	k.expireProcess(key)
}

func (k *KVStore) expireProcess(key string) {
	delete(k.expiredAt, key)
	delete(k.data, key)
	k.expireTimeWheel.Rem(key)
}

func (k *KVStore) expire(key string, expiredAt time.Time) {
	if _, ok := k.data[key]; !ok {
		return
	}
	k.expiredAt[key] = expiredAt
	k.expireTimeWheel.Add(expiredAt.Unix(), key)
}
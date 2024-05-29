package database

import (
	"context"
	"goredis/database"
	"goredis/handler"
	"goredis/lib"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type KVStore struct {
	data            map[string]interface{}
	expireAt        map[string]time.Time
	expireTimeWheel SortedSet
	persister       handler.Persister
}

func (k *KVStore) Get(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	v, err := k.getAsString(key)
	return handler.NewBulkReply(v.Bytes())
}

func (k *KVStore) getAsString(key string) (String, error) {
	v, ok := k.data[key]

	str, ok := v.(String)

	return str, nil
}

func (k *KVStore) Set(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	value := string(args[1])

	// 支持 NX EX
	var (
		insertStrategy bool
		ttlStrategy    bool
		ttlSeconds     int64
		ttlIndex       = -1
	)

	for i := 2; i < len(args); i++ {
		flag := strings.ToLower(string(args[i]))
		switch flag {
		// 处理带 only insert 模式的 nx 指令
		case "nx":
			insertStrategy = true
		// 处理带过期时间的 ex 指令
		case "ex":
			// 重复的 ex 指令
			if ttlStrategy {
				return handler.NewSyntaxErrReply()
			}
			if i == len(args)-1 {
				return handler.NewSyntaxErrReply()
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			// ...
			ttlStrategy = true
			ttlSeconds = ttl
			ttlIndex = i
			i++
			// ...
		}
	}

	if ttlIndex != -1 {
		args = append(args[:ttlIndex], args[ttlIndex+2:]...)
	}

	// 将 kv 数据写入到 data map
	affected := k.put(key, value, insertStrategy)
	// 过期时间设置
	if affected > 0 && ttlStrategy {
		expireAt := lib.TimeNow().Add(time.Duration(ttlSeconds) * time.Second)
		_cmd := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
		_ = k.expireAt(cmd.Ctx(), _cmd, key, expireAt) // 其中会完成 ex 信息的持久化
	}

	// 过期时间处理
	if affected > 0 {
		// 指令持久化
		k.persister.PersistCmd(cmd.Ctx(), append([][]byte{[]byte(database.CmdTypeSet)}, args...))
		return handler.NewIntReply(affected)
	}

	return handler.NewNillReply()
}

func (k *KVStore) put(key, value string, insertStrategy bool) int64 {
	if _, ok := k.data[key]; ok && insertStrategy {
		return 0
	}

	k.data[key] = NewString(key, value)
	return 1
}

func (k *KVStore) Expire(cmd *database.Command) handler.Reply {
	args := cmd.Args()
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)

	expireAt := lib.TimeNow().Add(time.Duration(ttl) * time.Second)
	_cmd := [][]byte{[]byte(database.CmdTypeExpireAt), []byte(key), []byte(lib.TimeSecondFormat(expireAt))}
	return k.expireAt(cmd.Ctx(), _cmd, key, expireAt)
}

func (k *KVStore) ExpireAt(ctx context.Context, cmd [][]byte, key string,  expireAt time.Time) handler.Reply  {
	k.expire(key, expireAt)
	k.persister.PersistCmd(ctx, cmd)
	return handler.NewOKReply()
}

func (k *KVStore) expire(key string, expireAt time.Time)  {
	if _, ok := k.data[key]; !ok {
		return
	}

	k.expireAt[key] = expireAt
	k.expireTimeWheel.Add(expireAt.Unix(), key)
}

func (k *KVStore) GC()  {
	nowUnix := lib.TimeNow().Unix()
	for _, expiredKey := range k.expireTimeWheel.Range(0, nowUnix) {
		k.expireProcess(expiredKey)
	}
}

func (k *KVStore) expireProcess(key string)  {
	delete(k.expireAt, key)
	delete(k.data, key)
	k.expireTimeWheel.Rem(key)
}

func (k *KVStore) ExpirePreprocess(key string)  {
	expiredAt, ok := k.expireAt[key]
	if !ok {
		return
	}

	if expiredAt.After(lib.TimeNow()) {
		return
	}

	k.expireProcess(key)
}

type String interface {
	Bytes() []byte
	database.CmdAdapter
}

type stringEntity struct {
	key, str string
}

func (s *stringEntity) Bytes() []byte {
	return []byte(s.str)
}

type List interface {
	LPush(value []byte)
	LPop(cnt int64) [][]byte
	RPush(value []byte)
	RPop(cnt int64) [][]byte
	Len() int64
	Range(start, stop int64) [][]byte
	database.CmdAdapter
}

type listEntity struct {
	key  string
	data [][]byte
}

func (l *listEntity) LPush(value []byte) {
	l.data = append([][]byte{value}, l.data...)
}

func (l *listEntity) RPop(cnt int64) [][]byte {
	if int64(len(l.data)) < cnt {
		return nil
	}

	poped := l.data[:cnt]
	l.data = l.data[cnt:]
	return poped
}

func (l *listEntity) Len() int64 {
	return int64(len(l.data))
}

func (l *listEntity) Range(start, stop int64) [][]byte {
	if stop == -1 {
		stop = int64(len(l.data) - 1)
	}

	if start < 0 || start >= int64(len(l.data)) {
		return nil
	}

	if stop < 0 || stop >= int64(len(l.data)) || stop < start {
		return nil
	}

	return l.data[start : stop+1]
}

type Set interface {
	Add(value string) int64
	Exist(value string) int64
	Rem(value string) int64
	database.CmdAdapter
}

type setEntity struct {
	key       string
	container map[string]struct{}
}

func (s *setEntity) Add(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 0
	}
	s.container[value] = struct{}{}
	return 1
}

func (s *setEntity) Exist(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 1
	}
	return 0
}

func (s *setEntity) Rem(value string) int64 {
	if _, ok := s.container[value]; ok {
		delete(s.container, value)
		return 1
	}
	return 0
}

type HashMap interface {
	Put(key string, value []byte)
	Get(key string) []byte
	Del(key string) int64
	database.CmdAdapter
}

type hashMapEntity struct {
	key  string
	data map[string][]byte
}

func (h *hashMapEntity) Put(key string, value []byte) {
	h.data[key] = value
}

func (h *hashMapEntity) Get(key string) []byte {
	return h.data[key]
}

func (h *hashMapEntity) Del(key string) int64 {
	if _, ok := h.data[key]; !ok {
		return 0
	}
	delete(h.data, key)
	return 1
}

type SortedSet interface {
	Add(score int64, member string)
	Rem(member string) int64
	Range(score1, score2 int64) []string
	database.CmdAdapter
}

type skiplist struct {
	key           string
	scoreToNode   map[int64]*skipnode
	memberToScore map[string]int64
	head          *skipnode
	rander        *rand.Rand
}

type skipnode struct {
	score   int64
	members map[string]struct{}
	nexts   []*skipnode
}

func (s *skiplist) Add(score int64, member string) {
	oldScore, ok := s.memberToScore[member]
	if ok {
		if oldScore == score {
			return
		}
		s.rem(oldScore, member)
	}

	s.memberToScore[member] = score
	node, ok := s.scoreToNode[score]
	if ok {
		node.members[member] = struct{}{}
		return
	}

	height := s.roll()
	for int64(len(s.head.nexts)) < height+1 {
		s.head.nexts = append(s.head.nexts, nil)
	}

	inserted := newSkipnode(score, height+1)
	inserted.member[member] = struct{}{}
	s.scoreToNode[score] = inserted

	move := s.head
	for i := height; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score {
			move = move.nexts[i]
			continue
		}

		inserted.nexts[i] = move.nexts[i]
		move.nexts[i] = inserted
	}
}

func (s *skiplist) roll() int64 {
	var level int64
	for s.rander.Intn(2) > 0 {
		level++
	}
	return level
}

func (s *skiplist) Rem(member string) int64 {
	score, ok := s.memberToScore[member]
	if !ok {
		return 0
	}
	s.rem(score, member)
	return 1
}

func (s *skiplist) rem(score int64, member string) {
	// 删除 member 与 score 的映射关系
	delete(s.memberToScore, member)
	// 获取 member 所从属的跳表节点
	skipnode := s.scoreToNode[score]

	delete(skipnode.members, member)
	if len(skipnode.members) > 0 {
		return
	}

	delete(s.scoreToNode, score)
	move := s.head
	for i := len(s.head.nexts) - 1; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score {
			move = move.nexts[i]
		}

		if move.nexts[i] == nil || move.nexts[i].score > score {
			continue
		}

		remed := move.nexts[i]
		move.nexts[i] = move.nexts[i].nexts[i]
		remed.nexts[i] = nil
	}
}

func (s *skiplist) Range(score1, score2 int64) []string {
	if score2 == -1 {
		score2 = math.MaxInt64
	}

	if score1 > score2 {
		return []string{}
	}

	move := s.head
	for i := len(s.head.nexts) - 1; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score1 {
			move = move.nexts[i]
		}
	}

	if len(move.nexts) == 0 || move.nexts[0] == nil {
		return []string{}
	}

	res := []string{}
	for move.nexts[0] != nil && move.nexts[0].score >= score1 && move.nexts[0].score <= score2 {
		for member := range move.nexts[0].members {
			res = append(res, member)
		}
		move = move.nexts[0]
	}
	return res
}

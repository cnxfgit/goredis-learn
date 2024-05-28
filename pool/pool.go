package pool

import (
	"github.com/panjf2000/ants"
)

var pool *ants.Pool

func init()  {
	_pool, err := ants.NewPool(50000, ants.WithPanicHandler(func (i interface{})  {
		
	}))

	if err != nil {
		panic(err)
	}
	pool = _pool
}

func Submit(task func())  {
	pool.Submit(task)
}
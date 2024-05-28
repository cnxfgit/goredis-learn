package handler

import (
	"context"
	"io"
)

type Persister interface {
	Reloader() (io.ReadCloser, error)
	PersistCmd(ctx context.Context, cmd [][]byte)
	Close()
}
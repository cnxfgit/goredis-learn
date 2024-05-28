package persist

import (
	"os"
	"sync/atomic"
)

type aofPersister struct {
	buffer                 chan [][]byte
	aofFile                *os.File
	aofFileName            string
	appendFsync            appendSyncStrategy
	autoAofRewriteAfterCmd int64
	aofCounter             atomic.Int64
}

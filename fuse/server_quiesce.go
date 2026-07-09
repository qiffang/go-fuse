package fuse

import (
	"context"
	"errors"
	"sync"
)

// ErrQuiesceAlreadyActive is returned when a second quiesce is requested while
// the server is already quiesced or quiescing.
var ErrQuiesceAlreadyActive = errors.New("fuse: quiesce already active")

// QuiesceToken keeps request dispatch paused until Resume is called.
type QuiesceToken struct {
	server *Server
	once   sync.Once
}

func (ms *Server) initQuiesceLocked() {
	if ms.quiesceCond == nil {
		ms.quiesceCond = sync.NewCond(&ms.quiesceMu)
	}
}

// Quiesce pauses new request dispatch and waits for already-dispatched
// requests to finish. The returned token must be resumed when the caller is
// ready to allow dispatch again.
func (ms *Server) Quiesce(ctx context.Context) (*QuiesceToken, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	ms.quiesceMu.Lock()
	ms.initQuiesceLocked()
	if ms.quiescing {
		ms.quiesceMu.Unlock()
		return nil, ErrQuiesceAlreadyActive
	}
	ms.quiescing = true

	stopWake := context.AfterFunc(ctx, func() {
		ms.quiesceMu.Lock()
		ms.quiesceCond.Broadcast()
		ms.quiesceMu.Unlock()
	})
	defer stopWake()

	for ms.inflightRequests > 0 {
		if err := ctx.Err(); err != nil {
			ms.quiescing = false
			ms.quiesceCond.Broadcast()
			ms.quiesceMu.Unlock()
			return nil, err
		}
		ms.quiesceCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		ms.quiescing = false
		ms.quiesceCond.Broadcast()
		ms.quiesceMu.Unlock()
		return nil, err
	}

	ms.quiesceMu.Unlock()
	return &QuiesceToken{server: ms}, nil
}

// Resume allows request dispatch to continue. It is safe to call multiple
// times.
func (t *QuiesceToken) Resume() {
	if t == nil || t.server == nil {
		return
	}
	t.once.Do(func() {
		t.server.quiesceMu.Lock()
		t.server.initQuiesceLocked()
		t.server.quiescing = false
		t.server.quiesceCond.Broadcast()
		t.server.quiesceMu.Unlock()
	})
}

// Close implements io.Closer for callers that prefer defer token.Close().
func (t *QuiesceToken) Close() error {
	t.Resume()
	return nil
}

func (ms *Server) beginRequestDispatch() {
	ms.quiesceMu.Lock()
	ms.initQuiesceLocked()
	for ms.quiescing {
		ms.quiesceCond.Wait()
	}
	ms.inflightRequests++
	ms.quiesceMu.Unlock()
}

func (ms *Server) endRequestDispatch() {
	ms.quiesceMu.Lock()
	ms.initQuiesceLocked()
	ms.inflightRequests--
	if ms.inflightRequests == 0 {
		ms.quiesceCond.Broadcast()
	}
	ms.quiesceMu.Unlock()
}

func quiesceTracksOpcode(opcode uint32) bool {
	return opcode != _OP_INTERRUPT
}

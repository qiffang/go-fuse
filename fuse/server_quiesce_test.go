package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestQuiesceWaitsForInflightAndBlocksDispatch(t *testing.T) {
	srv := &Server{}
	srv.beginRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()

	waitForQuiescing(t, srv, true)
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)

	dispatchCh := make(chan struct{})
	go func() {
		srv.beginRequestDispatch()
		close(dispatchCh)
		srv.endRequestDispatch()
	}()
	assertBlocked(t, dispatchCh)

	token.Resume()
	waitForClosed(t, dispatchCh, "dispatch")
}

func TestQuiesceRejectsSecondActiveQuiesce(t *testing.T) {
	srv := &Server{}
	token, err := srv.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer token.Resume()

	if _, err := srv.Quiesce(context.Background()); !errors.Is(err, ErrQuiesceAlreadyActive) {
		t.Fatalf("Quiesce error = %v, want ErrQuiesceAlreadyActive", err)
	}
}

func TestQuiesceContextCancelRollsBackPause(t *testing.T) {
	srv := &Server{}
	srv.beginRequestDispatch()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.Quiesce(ctx)
		errCh <- err
	}()

	waitForQuiescing(t, srv, true)
	cancel()
	if err := waitForErr(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("Quiesce error = %v, want context.Canceled", err)
	}
	waitForQuiescing(t, srv, false)

	dispatchCh := make(chan struct{})
	go func() {
		srv.beginRequestDispatch()
		close(dispatchCh)
		srv.endRequestDispatch()
	}()
	waitForClosed(t, dispatchCh, "dispatch after cancel")

	srv.endRequestDispatch()
}

func TestQuiesceTracksRequestOpcodes(t *testing.T) {
	if quiesceTracksOpcode(_OP_INTERRUPT) {
		t.Fatal("interrupt requests must bypass quiesce to avoid drain deadlocks")
	}
	if !quiesceTracksOpcode(_OP_FORGET) {
		t.Fatal("forget requests must be tracked until side effects run")
	}
	if !quiesceTracksOpcode(_OP_BATCH_FORGET) {
		t.Fatal("batch forget requests must be tracked until side effects run")
	}
	if !quiesceTracksOpcode(_OP_NOTIFY_REPLY) {
		t.Fatal("notify reply requests must be tracked until side effects run")
	}
	if !quiesceTracksOpcode(_OP_LOOKUP) {
		t.Fatal("filesystem requests must be tracked by quiesce")
	}
}

func TestQuiesceInterruptOnlyGateOpcodes(t *testing.T) {
	if !quiesceAllowsInterruptOnlyOpcode(_OP_INTERRUPT) {
		t.Fatal("interrupt requests must pass interrupt-only mode")
	}
	if !quiesceAllowsInterruptOnlyOpcode(_OP_FORGET) {
		t.Fatal("forget requests must pass interrupt-only mode because the kernel cannot retry them")
	}
	if !quiesceAllowsInterruptOnlyOpcode(_OP_BATCH_FORGET) {
		t.Fatal("batch forget requests must pass interrupt-only mode because the kernel cannot retry them")
	}
	if !quiesceAllowsInterruptOnlyOpcode(_OP_NOTIFY_REPLY) {
		t.Fatal("notify reply requests must pass interrupt-only mode because the kernel cannot retry them")
	}
	if quiesceAllowsInterruptOnlyOpcode(_OP_LOOKUP) {
		t.Fatal("reply-expected filesystem requests must be rejected in interrupt-only mode")
	}
}

func TestQuiesceBlocksFuseFdReads(t *testing.T) {
	fs := NewDefaultRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	token, err := srv.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	if _, err := syscall.Write(fds[1], makeFuseRequest(_OP_GETATTR, 42)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	assertNoReadRequest(t, reqCh, statusCh)

	token.Resume()
	req := waitForReadRequest(t, reqCh, statusCh)
	defer srv.returnRequest(req)
	if req.quiesceTracked {
		srv.endRequestDispatch()
		req.quiesceTracked = false
	}
	if got := req.inHeader().Opcode; got != _OP_GETATTR {
		t.Fatalf("opcode = %d, want GETATTR", got)
	}
}

func TestQuiesceBlocksReaderAlreadyWaitingOnFd(t *testing.T) {
	fs := NewDefaultRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	waitForInflightRequests(t, srv, 1)

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()

	token := waitForToken(t, tokenCh, errCh)
	defer token.Resume()

	if _, err := syscall.Write(fds[1], makeFuseRequest(_OP_GETATTR, 42)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	assertNoReadRequest(t, reqCh, statusCh)

	token.Resume()
	req := waitForReadRequest(t, reqCh, statusCh)
	defer srv.returnRequest(req)
	if req.quiesceTracked {
		srv.endRequestDispatch()
		req.quiesceTracked = false
	}
	if got := req.inHeader().Opcode; got != _OP_GETATTR {
		t.Fatalf("opcode = %d, want GETATTR", got)
	}
}

func TestQuiesceAllowsInterruptAfterIdleReaderParks(t *testing.T) {
	fs := NewDefaultRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()
	waitForInflightRequests(t, srv, 2)

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	time.Sleep(requestReadPollTimeout + 25*time.Millisecond)
	if _, err := syscall.Write(fds[1], makeFuseInterruptRequest(43, 42)); err != nil {
		t.Fatalf("write interrupt: %v", err)
	}

	req := waitForReadRequest(t, reqCh, statusCh)
	defer srv.returnRequest(req)
	if req.quiesceTracked {
		t.Fatal("interrupt request must not be tracked by quiesce")
	}
	if got := req.inHeader().Opcode; got != _OP_INTERRUPT {
		t.Fatalf("opcode = %d, want INTERRUPT", got)
	}
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func TestQuiesceRejectsNormalRequestWhileReadingInterrupts(t *testing.T) {
	fs := NewDefaultRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()
	waitForInflightRequests(t, srv, 2)

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	time.Sleep(requestReadPollTimeout + 25*time.Millisecond)
	if _, err := syscall.Write(fds[1], makeFuseRequest(_OP_GETATTR, 44)); err != nil {
		t.Fatalf("write getattr: %v", err)
	}

	out := waitForFuseReply(t, fds[1])
	if out.Unique != 44 {
		t.Fatalf("reply unique = %d, want 44", out.Unique)
	}
	if got := Status(-out.Status); got != EAGAIN {
		t.Fatalf("reply status = %v, want EAGAIN", got)
	}
	assertNoReadRequest(t, reqCh, statusCh)
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
	stopReadRequestWithInterrupt(t, srv, fds[1], reqCh, statusCh)
}

func TestQuiesceDispatchesForgetWhileReadingInterrupts(t *testing.T) {
	fs := newRecordingRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	if _, err := syscall.Write(fds[1], makeFuseForgetRequest(45, 1234, 5)); err != nil {
		t.Fatalf("write forget: %v", err)
	}

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	req := waitForReadRequest(t, reqCh, statusCh)
	if !req.quiesceTracked {
		t.Fatal("forget request must stay tracked until Forget runs")
	}
	if got := req.inHeader().Opcode; got != _OP_FORGET {
		t.Fatalf("opcode = %d, want FORGET", got)
	}
	srv.handleRequest(req)
	fs.waitForget(t, forgetCall{nodeID: 1234, nlookup: 5})
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func TestQuiesceDispatchesBatchForgetWhileReadingInterrupts(t *testing.T) {
	fs := newRecordingRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	calls := []forgetCall{
		{nodeID: 1234, nlookup: 5},
		{nodeID: 5678, nlookup: 9},
	}
	if _, err := syscall.Write(fds[1], makeFuseBatchForgetRequest(46, calls)); err != nil {
		t.Fatalf("write batch forget: %v", err)
	}

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	req := waitForReadRequest(t, reqCh, statusCh)
	if !req.quiesceTracked {
		t.Fatal("batch forget request must stay tracked until Forget runs")
	}
	if got := req.inHeader().Opcode; got != _OP_BATCH_FORGET {
		t.Fatalf("opcode = %d, want BATCH_FORGET", got)
	}
	srv.handleRequest(req)
	for _, call := range calls {
		fs.waitForget(t, call)
	}
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func TestQuiesceWaitsForReadForgetDispatch(t *testing.T) {
	fs := newRecordingRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	if _, err := syscall.Write(fds[1], makeFuseForgetRequest(47, 1234, 5)); err != nil {
		t.Fatalf("write forget: %v", err)
	}

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	req := waitForReadRequest(t, reqCh, statusCh)
	if !req.quiesceTracked {
		t.Fatal("forget request must stay tracked after read")
	}
	if got := req.inHeader().Opcode; got != _OP_FORGET {
		t.Fatalf("opcode = %d, want FORGET", got)
	}

	srv.endRequestDispatch()
	assertNoToken(t, tokenCh, errCh)

	srv.handleRequest(req)
	fs.waitForget(t, forgetCall{nodeID: 1234, nlookup: 5})
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func TestQuiesceWaitsForReadBatchForgetDispatch(t *testing.T) {
	fs := newRecordingRawFileSystem()
	opts := copyMountOptions(fs, nil)
	srv := newServer(fs, &opts)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	srv.mountFd = fds[0]
	srv.singleReader = true
	defer syscall.Close(srv.mountFd)
	defer syscall.Close(fds[1])

	srv.beginRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()
	waitForQuiescing(t, srv, true)

	calls := []forgetCall{
		{nodeID: 1234, nlookup: 5},
		{nodeID: 5678, nlookup: 9},
	}
	if _, err := syscall.Write(fds[1], makeFuseBatchForgetRequest(48, calls)); err != nil {
		t.Fatalf("write batch forget: %v", err)
	}

	reqCh := make(chan *requestAlloc, 1)
	statusCh := make(chan Status, 1)
	go func() {
		req, status := srv.readRequest()
		if status != OK {
			statusCh <- status
			return
		}
		reqCh <- req
	}()

	req := waitForReadRequest(t, reqCh, statusCh)
	if !req.quiesceTracked {
		t.Fatal("batch forget request must stay tracked after read")
	}
	if got := req.inHeader().Opcode; got != _OP_BATCH_FORGET {
		t.Fatalf("opcode = %d, want BATCH_FORGET", got)
	}

	srv.endRequestDispatch()
	assertNoToken(t, tokenCh, errCh)

	srv.handleRequest(req)
	for _, call := range calls {
		fs.waitForget(t, call)
	}
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func TestQuiesceDrainsAlreadyReadRequestBeforeReturning(t *testing.T) {
	srv := &Server{}
	srv.beginReadRequestDispatch()

	tokenCh := make(chan *QuiesceToken, 1)
	errCh := make(chan error, 1)
	go func() {
		token, err := srv.Quiesce(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		tokenCh <- token
	}()

	waitForQuiescing(t, srv, true)
	assertNoToken(t, tokenCh, errCh)

	srv.endRequestDispatch()
	token := waitForToken(t, tokenCh, errCh)
	token.Resume()
}

func makeFuseRequest(opcode uint32, unique uint64) []byte {
	req := make([]byte, int(unsafe.Sizeof(InHeader{})))
	in := (*InHeader)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = opcode
	in.Unique = unique
	return req
}

func makeFuseForgetRequest(unique uint64, nodeID uint64, nlookup uint64) []byte {
	req := make([]byte, int(unsafe.Sizeof(ForgetIn{})))
	in := (*ForgetIn)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = _OP_FORGET
	in.InHeader.Unique = unique
	in.NodeId = nodeID
	in.Nlookup = nlookup
	return req
}

func makeFuseBatchForgetRequest(unique uint64, calls []forgetCall) []byte {
	headerSize := int(unsafe.Sizeof(_BatchForgetIn{}))
	entrySize := int(unsafe.Sizeof(_ForgetOne{}))
	req := make([]byte, headerSize+entrySize*len(calls))
	in := (*_BatchForgetIn)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = _OP_BATCH_FORGET
	in.Unique = unique
	in.Count = uint32(len(calls))
	entries := unsafe.Slice((*_ForgetOne)(unsafe.Pointer(&req[headerSize])), len(calls))
	for i, call := range calls {
		entries[i] = _ForgetOne{
			NodeId:  call.nodeID,
			Nlookup: call.nlookup,
		}
	}
	return req
}

func makeFuseInterruptRequest(unique uint64, targetUnique uint64) []byte {
	req := make([]byte, int(unsafe.Sizeof(InterruptIn{})))
	in := (*InterruptIn)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = _OP_INTERRUPT
	in.InHeader.Unique = unique
	in.Unique = targetUnique
	return req
}

type forgetCall struct {
	nodeID  uint64
	nlookup uint64
}

type recordingRawFileSystem struct {
	RawFileSystem
	forgetCh chan forgetCall
}

func newRecordingRawFileSystem() *recordingRawFileSystem {
	return &recordingRawFileSystem{
		RawFileSystem: NewDefaultRawFileSystem(),
		forgetCh:      make(chan forgetCall, 4),
	}
}

func (fs *recordingRawFileSystem) Forget(nodeID, nlookup uint64) {
	fs.forgetCh <- forgetCall{nodeID: nodeID, nlookup: nlookup}
}

func (fs *recordingRawFileSystem) waitForget(t *testing.T, want forgetCall) {
	t.Helper()
	select {
	case got := <-fs.forgetCh:
		if got != want {
			t.Fatalf("forget call = %+v, want %+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for forget call %+v", want)
	}
}

func waitForQuiescing(t *testing.T, srv *Server, want bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.quiesceMu.Lock()
		got := srv.quiescing
		srv.quiesceMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("quiescing did not become %v", want)
}

func waitForInflightRequests(t *testing.T, srv *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		srv.quiesceMu.Lock()
		got := srv.inflightRequests
		srv.quiesceMu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("inflightRequests did not become %d", want)
}

func assertNoReadRequest(t *testing.T, reqCh <-chan *requestAlloc, statusCh <-chan Status) {
	t.Helper()
	select {
	case req := <-reqCh:
		t.Fatalf("readRequest returned while quiesced: opcode=%d", req.inHeader().Opcode)
	case status := <-statusCh:
		t.Fatalf("readRequest failed while quiesced: %v", status)
	case <-time.After(25 * time.Millisecond):
	}
}

func assertNoToken(t *testing.T, tokenCh <-chan *QuiesceToken, errCh <-chan error) {
	t.Helper()
	select {
	case token := <-tokenCh:
		token.Resume()
		t.Fatal("Quiesce returned before in-flight dispatch completed")
	case err := <-errCh:
		t.Fatalf("Quiesce failed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
}

func assertBlocked(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("dispatch was not blocked by active quiesce")
	case <-time.After(25 * time.Millisecond):
	}
}

func waitForToken(t *testing.T, tokenCh <-chan *QuiesceToken, errCh <-chan error) *QuiesceToken {
	t.Helper()
	select {
	case token := <-tokenCh:
		return token
	case err := <-errCh:
		t.Fatalf("Quiesce failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Quiesce")
	}
	return nil
}

func waitForReadRequest(t *testing.T, reqCh <-chan *requestAlloc, statusCh <-chan Status) *requestAlloc {
	t.Helper()
	select {
	case req := <-reqCh:
		return req
	case status := <-statusCh:
		t.Fatalf("readRequest failed: %v", status)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readRequest")
	}
	return nil
}

func stopReadRequestWithInterrupt(t *testing.T, srv *Server, fd int, reqCh <-chan *requestAlloc, statusCh <-chan Status) {
	t.Helper()
	if _, err := syscall.Write(fd, makeFuseInterruptRequest(99, 44)); err != nil {
		t.Fatalf("write interrupt to stop readRequest: %v", err)
	}
	req := waitForReadRequest(t, reqCh, statusCh)
	opcode := req.inHeader().Opcode
	tracked := req.quiesceTracked
	if tracked {
		srv.endRequestDispatch()
		req.quiesceTracked = false
	}
	srv.returnRequest(req)
	if opcode != _OP_INTERRUPT {
		t.Fatalf("stop request opcode = %d, want INTERRUPT", opcode)
	}
	if tracked {
		t.Fatal("stop interrupt request must not be tracked by quiesce")
	}
}

func waitForFuseReply(t *testing.T, fd int) OutHeader {
	t.Helper()
	replyCh := make(chan OutHeader, 1)
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, int(unsafe.Sizeof(OutHeader{})))
		n, err := syscall.Read(fd, buf)
		if err != nil {
			errCh <- err
			return
		}
		if n != len(buf) {
			errCh <- syscall.EINVAL
			return
		}
		replyCh <- *(*OutHeader)(unsafe.Pointer(&buf[0]))
	}()
	select {
	case out := <-replyCh:
		return out
	case err := <-errCh:
		t.Fatalf("read reply failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fuse reply")
	}
	return OutHeader{}
}

func waitForErr(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error")
	}
	return nil
}

func waitForClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

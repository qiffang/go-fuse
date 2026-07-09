package fuse

import (
	"context"
	"errors"
	"testing"
	"time"
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
	if !quiesceTracksOpcode(_OP_LOOKUP) {
		t.Fatal("filesystem requests must be tracked by quiesce")
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

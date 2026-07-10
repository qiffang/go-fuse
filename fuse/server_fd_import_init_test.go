package fuse

import (
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// testGetAttrFS is a minimal RawFileSystem that handles GETATTR for root.
type testGetAttrFS struct {
	defaultRawFileSystem
}

func (fs *testGetAttrFS) GetAttr(cancel <-chan struct{}, in *GetAttrIn, out *AttrOut) Status {
	if in.NodeId == 1 {
		out.Attr.Mode = 0755 | syscall.S_IFDIR
		out.Attr.Ino = 1
		return OK
	}
	return ENOENT
}

// TestImportFdWithInitRejectsNegativeFd verifies the fd validation.
func TestImportFdWithInitRejectsNegativeFd(t *testing.T) {
	ks := InitIn{Major: _FUSE_KERNEL_VERSION, Minor: _OUR_MINOR_VERSION}
	_, err := ImportFdWithInit(NewDefaultRawFileSystem(), t.TempDir(), -1, &ks, nil)
	if err == nil {
		t.Fatal("ImportFdWithInit should reject negative fd")
	}
}

// TestImportFdWithInitRejectsNilKernelSettings verifies the nil check.
func TestImportFdWithInitRejectsNilKernelSettings(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])

	_, err = ImportFdWithInit(NewDefaultRawFileSystem(), t.TempDir(), fds[0], nil, nil)
	if err == nil {
		t.Fatal("ImportFdWithInit should reject nil kernelSettings")
	}
}

// TestImportFdWithInitServesGetAttr proves that a server created via
// ImportFdWithInit (with pre-provided kernel settings, no INIT handshake)
// can correctly serve a normal FUSE GETATTR request.
//
// Flow:
//  1. Create a socketpair (simulates FUSE device fd)
//  2. Build kernel settings as if INIT already completed
//  3. Call ImportFdWithInit — no INIT request on the wire
//  4. Start Serve() in background
//  5. Write a raw GETATTR request for root (nodeId=1)
//  6. Read back the response and verify Status == 0
func TestImportFdWithInitServesGetAttr(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	serverFd, peerFd := fds[0], fds[1]
	defer syscall.Close(peerFd)

	// Simulate kernel settings from a completed INIT negotiation.
	ks := InitIn{
		Major:        _FUSE_KERNEL_VERSION,
		Minor:        _OUR_MINOR_VERSION,
		MaxReadAhead: 4096,
	}

	mountPoint := t.TempDir()
	srv, err := ImportFdWithInit(&testGetAttrFS{}, mountPoint, serverFd, &ks, nil)
	if err != nil {
		t.Fatalf("ImportFdWithInit: %v", err)
	}

	// Serve in background — reads from serverFd.
	serveDone := make(chan struct{})
	go func() {
		srv.Serve()
		close(serveDone)
	}()

	// Build a raw GETATTR request for root (nodeId=1).
	req := makeGetAttrRequest(100, 1)
	if _, err := syscall.Write(peerFd, req); err != nil {
		t.Fatalf("write GETATTR request: %v", err)
	}

	// Read the response.
	resp := make([]byte, int(unsafe.Sizeof(OutHeader{}))+int(unsafe.Sizeof(AttrOut{})))
	n, err := syscall.Read(peerFd, resp)
	if err != nil {
		t.Fatalf("read GETATTR response: %v", err)
	}
	if n < int(unsafe.Sizeof(OutHeader{})) {
		t.Fatalf("response too short: %d bytes", n)
	}

	out := (*OutHeader)(unsafe.Pointer(&resp[0]))
	if out.Unique != 100 {
		t.Fatalf("response unique = %d, want 100", out.Unique)
	}
	if out.Status != 0 {
		t.Fatalf("response status = %d, want 0 (success)", out.Status)
	}

	// Close peer fd to make Serve() exit.
	_ = syscall.Close(peerFd)

	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not exit after closing peer fd")
	}
}

func makeGetAttrRequest(unique uint64, nodeId uint64) []byte {
	req := make([]byte, int(unsafe.Sizeof(GetAttrIn{})))
	in := (*GetAttrIn)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = _OP_GETATTR
	in.Unique = unique
	in.NodeId = nodeId
	return req
}

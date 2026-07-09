package fuse

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

func TestExportFdDuplicatesServerFd(t *testing.T) {
	pipe := make([]int, 2)
	if err := syscall.Pipe(pipe); err != nil {
		t.Fatal(err)
	}
	readFd, writeFd := pipe[0], pipe[1]
	defer syscall.Close(readFd)

	srv := &Server{mountFd: writeFd}
	dupFd, err := srv.ExportFd()
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(dupFd)

	if err := syscall.Close(writeFd); err != nil {
		t.Fatal(err)
	}
	srv.mountFd = -1

	if _, err := syscall.Write(dupFd, []byte("x")); err != nil {
		t.Fatalf("write through duplicate fd: %v", err)
	}
	buf := make([]byte, 1)
	n, err := syscall.Read(readFd, buf)
	if err != nil {
		t.Fatalf("read from pipe: %v", err)
	}
	if n != 1 || buf[0] != 'x' {
		t.Fatalf("read %q (%d bytes), want x", buf[:n], n)
	}
}

func TestExportFdClosedServer(t *testing.T) {
	srv := &Server{mountFd: -1}
	if _, err := srv.ExportFd(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ExportFd error = %v, want os.ErrClosed", err)
	}
}

func TestImportFdRejectsNegativeFd(t *testing.T) {
	if _, err := ImportFd(NewDefaultRawFileSystem(), t.TempDir(), -1, nil); err == nil {
		t.Fatal("ImportFd succeeded with a negative fd")
	}
}

func TestImportFdUsesProvidedFdAndMountPoint(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	serverFd, peerFd := fds[0], fds[1]
	defer syscall.Close(peerFd)

	req := makeInitRequest(42)
	if _, err := syscall.Write(peerFd, req); err != nil {
		t.Fatalf("write init request: %v", err)
	}

	mountPoint := filepath.Join(t.TempDir(), "mnt")
	srv, err := ImportFd(NewDefaultRawFileSystem(), mountPoint, serverFd, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(srv.mountFd)

	if srv.mountFd != serverFd {
		t.Fatalf("mountFd = %d, want imported fd %d", srv.mountFd, serverFd)
	}
	if srv.mountPoint != mountPoint {
		t.Fatalf("mountPoint = %q, want %q", srv.mountPoint, mountPoint)
	}

	resp := make([]byte, int(sizeOfOutHeader)+int(unsafe.Sizeof(InitOut{})))
	n, err := syscall.Read(peerFd, resp)
	if err != nil {
		t.Fatalf("read init response: %v", err)
	}
	if n < int(sizeOfOutHeader) {
		t.Fatalf("init response too short: %d bytes", n)
	}
	out := (*OutHeader)(unsafe.Pointer(&resp[0]))
	if out.Status != 0 {
		t.Fatalf("init status = %d, want 0", out.Status)
	}
	if out.Unique != 42 {
		t.Fatalf("init unique = %d, want 42", out.Unique)
	}
}

func makeInitRequest(unique uint64) []byte {
	req := make([]byte, int(unsafe.Sizeof(InitIn{})))
	in := (*InitIn)(unsafe.Pointer(&req[0]))
	in.Length = uint32(len(req))
	in.Opcode = _OP_INIT
	in.Unique = unique
	in.Major = _FUSE_KERNEL_VERSION
	in.Minor = _OUR_MINOR_VERSION
	in.MaxReadAhead = 4096
	return req
}

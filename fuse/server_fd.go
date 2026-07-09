package fuse

import (
	"fmt"
	"os"
	"syscall"
)

// ExportFd duplicates the FUSE device fd owned by the server.
//
// The returned fd is owned by the caller and must be closed by the caller. The
// server keeps its original fd open and continues to own it. The caller also
// controls any close-on-exec policy for the duplicate.
func (ms *Server) ExportFd() (int, error) {
	ms.writeMu.Lock()
	defer ms.writeMu.Unlock()

	if ms.mountFd < 0 {
		return -1, fmt.Errorf("fuse: export fd: %w", os.ErrClosed)
	}
	fd, err := syscall.Dup(ms.mountFd)
	if err != nil {
		return -1, os.NewSyscallError("dup", err)
	}
	return fd, nil
}

// ImportFd creates a FUSE server around an already-mounted FUSE device fd.
//
// On success the server takes ownership of fd and will close it when Serve
// exits. On failure ImportFd closes fd before returning. mountPoint is the real
// mounted directory path used for WaitMount and Unmount; ImportFd does not call
// mount(2).
func ImportFd(fs RawFileSystem, mountPoint string, fd int, opts *MountOptions) (*Server, error) {
	if fd < 0 {
		return nil, fmt.Errorf("fuse: import fd: invalid fd %d", fd)
	}

	o := copyMountOptions(fs, opts)
	ms := newServer(fs, &o)

	mountPoint, err := normalizeMountPoint(mountPoint)
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}

	syscall.CloseOnExec(fd)
	close(ms.ready)
	if err := ms.attachFd(mountPoint, fd); err != nil {
		return nil, err
	}
	return ms, nil
}

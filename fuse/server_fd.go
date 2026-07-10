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

// ImportFdWithInit creates a FUSE server around an already-mounted,
// already-initialized FUSE device fd. Unlike ImportFd, it skips the
// INIT handshake and uses the provided kernel settings from the
// exporting server. This is required for live fd handoff between
// processes: the kernel does not re-send INIT on a connection that
// has already been initialized.
//
// kernelSettings must be the InitIn from the original INIT negotiation,
// as returned by Server.KernelSettings() on the exporting server.
//
// On success the server takes ownership of fd and will close it when
// Serve exits. On failure ImportFdWithInit closes fd before returning.
func ImportFdWithInit(fs RawFileSystem, mountPoint string, fd int, kernelSettings *InitIn, opts *MountOptions) (*Server, error) {
	if fd < 0 {
		return nil, fmt.Errorf("fuse: import fd: invalid fd %d", fd)
	}
	if kernelSettings == nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("fuse: import fd: kernelSettings is required for initialized connections")
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

	// Apply kernel settings from the original INIT negotiation.
	ms.kernelSettings = *kernelSettings

	// Set splice if kernel supports it.
	if ms.kernelSettings.Minor >= 13 {
		ms.setSplice()
	}

	// Attach mountPoint and fd before Init so the filesystem sees
	// a fully attached server, matching ImportFd/NewServer invariant.
	ms.mountPoint = mountPoint
	ms.mountFd = fd
	ms.loops.Add(1)

	// Initialize the filesystem (after attach, so Init can use server state).
	ms.fileSystem.Init(ms)

	return ms, nil
}

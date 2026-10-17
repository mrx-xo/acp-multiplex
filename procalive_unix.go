//go:build !windows

package main

import "syscall"

// processAlive reports whether PID still exists.  Signal 0 probes without
// delivering anything; EPERM still means the process is there.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

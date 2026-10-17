//go:build windows

package main

import "syscall"

const processQueryLimitedInformation = 0x1000

// processAlive reports whether PID still exists.  Windows has no signal 0;
// opening the process with the weakest query right and checking its exit
// code is the equivalent probe.  A handle we cannot open (access denied)
// still means the process is there.
func processAlive(pid int) bool {
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return err == syscall.ERROR_ACCESS_DENIED
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return true
	}
	const stillActive = 259
	return code == stillActive
}

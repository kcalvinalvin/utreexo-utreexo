//go:build unix && !linux

package utreexo

import "syscall"

// mmapFlags are the flags passed to syscall.Mmap for shared, read-write mappings.
// MAP_POPULATE is Linux-only, so other platforms use MAP_SHARED alone.
const mmapFlags = syscall.MAP_SHARED

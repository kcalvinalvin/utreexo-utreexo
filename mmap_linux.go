package utreexo

import "syscall"

// mmapFlags are the flags passed to syscall.Mmap for shared, read-write mappings.
// Unlike the swisstable package, MAP_POPULATE is not used here because the
// forest maps very large sparse files (~256 GB) where pre-faulting all pages
// would be prohibitively slow. The OS pages in data on demand instead.
const mmapFlags = syscall.MAP_SHARED

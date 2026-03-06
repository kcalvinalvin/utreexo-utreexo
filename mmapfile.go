//go:build unix

package utreexo

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

// Assert that mmapFile implements walTarget (and therefore forestFile).
var _ walTarget = (*mmapFile)(nil)

// mmapFile implements walTarget over a memory-mapped file. The full
// maxSize is mapped upfront over a sparse file; only written pages
// consume physical memory and disk.
//
// Writes go to an internal dirty buffer (cacheStore), NOT directly
// to the mmap. This preserves WAL crash atomicity: the mmap (and
// therefore the file) is only updated during applyDirty, after the
// WAL journal has been synced.
//
// Reads check the dirty buffer first, then fall through to the mmap.
type mmapFile struct {
	file    *os.File
	data    []byte // mmap'd region (read-only access until applyDirty)
	pos     int64
	maxSize int64

	// Dirty write buffer — replaces cachedRWS for mmap-backed files.
	dirty      cacheStore
	maxWritten int64 // highest byte offset written (for SeekEnd)
	baseSize   int64 // logicalSize at last reset (for EOF on reads)
}

// newMmapFile creates an mmapFile by extending f to maxSize (as a sparse
// file) and mapping the entire range. entrySize (4, 8, or 32) and
// maxCacheBytes configure the dirty write buffer.
func newMmapFile(f *os.File, maxSize int64, entrySize int, maxCacheBytes int64) (*mmapFile, error) {
	if int64(int(maxSize)) != maxSize {
		return nil, fmt.Errorf("mmapFile: maxSize %d exceeds addressable range", maxSize)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("mmapFile stat: %w", err)
	}
	logicalSize := fi.Size()

	// Extend the file to maxSize. On Linux this creates a sparse file;
	// only pages that are actually written consume disk blocks.
	if err := f.Truncate(maxSize); err != nil {
		return nil, fmt.Errorf("mmapFile truncate to %d: %w", maxSize, err)
	}

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(maxSize),
		syscall.PROT_READ|syscall.PROT_WRITE, mmapFlags)
	if err != nil {
		f.Truncate(logicalSize) // best-effort restore
		return nil, fmt.Errorf("mmapFile mmap: %w", err)
	}

	if maxCacheBytes <= 0 {
		maxCacheBytes = defaultMaxCacheMemory
	}

	dirty, err := newCacheStore(entrySize, maxCacheBytes)
	if err != nil {
		syscall.Munmap(mmapData)
		f.Truncate(logicalSize)
		return nil, err
	}

	return &mmapFile{
		file:       f,
		data:       mmapData,
		maxSize:    maxSize,
		dirty:      dirty,
		maxWritten: logicalSize,
		baseSize:   logicalSize,
	}, nil
}

// ReadAt checks the dirty buffer first, then reads from the mmap.
func (m *mmapFile) ReadAt(p []byte, off int64) (int, error) {
	if cached, ok := m.dirty.get(off); ok {
		n := copy(p, cached)
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	if off >= m.baseSize {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > m.baseSize {
		n := copy(p, m.data[off:m.baseSize])
		return n, io.EOF
	}
	return copy(p, m.data[off:end]), nil
}

// Read checks the dirty buffer, then falls through to the mmap.
func (m *mmapFile) Read(p []byte) (int, error) {
	if cached, ok := m.dirty.get(m.pos); ok {
		n := copy(p, cached)
		m.pos += int64(n)
		return n, nil
	}
	if m.pos >= m.baseSize {
		// Beyond storage: return zeros.
		for i := range p {
			p[i] = 0
		}
		n := len(p)
		m.pos += int64(n)
		return n, nil
	}
	end := m.pos + int64(len(p))
	if end > m.baseSize {
		end = m.baseSize
	}
	n := copy(p, m.data[m.pos:end])
	m.pos += int64(n)
	return n, nil
}

// Write stores data in the dirty buffer at the current position.
func (m *mmapFile) Write(p []byte) (int, error) {
	switch cache := m.dirty.(type) {
	case *cacheMap4:
		if len(p) != 4 {
			return 0, fmt.Errorf("expected 4 bytes, got %d", len(p))
		}
		cache.put4(m.pos, [4]byte(p))
	case *cacheMap8:
		if len(p) != 8 {
			return 0, fmt.Errorf("expected 8 bytes, got %d", len(p))
		}
		cache.put8(m.pos, [8]byte(p))
	case *cacheMap32:
		if len(p) != 32 {
			return 0, fmt.Errorf("expected 32 bytes, got %d", len(p))
		}
		cache.put32(m.pos, [32]byte(p))
	default:
		return 0, fmt.Errorf("unsupported cache type %T", m.dirty)
	}

	n := len(p)
	m.pos += int64(n)
	if m.pos > m.maxWritten {
		m.maxWritten = m.pos
	}
	return n, nil
}

func (m *mmapFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.pos = offset
	case io.SeekCurrent:
		m.pos += offset
	case io.SeekEnd:
		m.pos = m.maxWritten + offset
	default:
		return 0, fmt.Errorf("mmapFile: invalid whence %d", whence)
	}
	return m.pos, nil
}

func (m *mmapFile) Sync() error {
	return m.file.Sync()
}

func (m *mmapFile) Truncate(size int64) error {
	m.dirty.deleteAbove(size)
	m.baseSize = size
	if m.maxWritten > size {
		m.maxWritten = size
	}
	if m.pos > size {
		m.pos = size
	}
	return nil
}

// forEachDirty iterates over all dirty entries.
func (m *mmapFile) forEachDirty(fn func(offset int64, data []byte)) {
	m.dirty.forEach(fn)
}

func (m *mmapFile) dirtyCount() int     { return m.dirty.count() }
func (m *mmapFile) dirtyEntrySize() int { return m.dirty.entrySize() }
func (m *mmapFile) flushNeeded() bool   { return m.dirty.overflowed() }

// applyDirty copies all dirty entries into the mmap.
func (m *mmapFile) applyDirty() error {
	var highestEnd int64
	m.dirty.forEach(func(offset int64, data []byte) {
		copy(m.data[offset:offset+int64(len(data))], data)
		end := offset + int64(len(data))
		if end > highestEnd {
			highestEnd = end
		}
	})
	return nil
}

// syncUnderlying fsyncs the underlying file (flushes MAP_SHARED pages).
func (m *mmapFile) syncUnderlying() error {
	return m.file.Sync()
}

// resetAfterFlush clears the dirty buffer and updates size tracking.
func (m *mmapFile) resetAfterFlush() error {
	m.dirty.clear()
	// After applyDirty, the mmap has the latest data up to maxWritten.
	m.baseSize = m.maxWritten
	m.pos = 0
	return nil
}

// discard drops all dirty entries without applying them.
func (m *mmapFile) discard() {
	m.dirty.clear()
	m.maxWritten = m.baseSize
	m.pos = 0
}

// Close applies any remaining dirty entries, syncs, unmaps, and closes.
func (m *mmapFile) Close() error {
	if err := syscall.Munmap(m.data); err != nil {
		m.file.Close()
		return fmt.Errorf("mmapFile munmap: %w", err)
	}
	// Restore the file to its logical size so that stat reports
	// the correct size on next open.
	if err := m.file.Truncate(m.maxWritten); err != nil {
		m.file.Close()
		return fmt.Errorf("mmapFile truncate on close: %w", err)
	}
	return m.file.Close()
}

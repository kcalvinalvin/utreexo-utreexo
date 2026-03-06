//go:build !unix

package utreexo

import (
	"fmt"
	"io"
	"os"
)

// Assert that mmapFile implements walTarget (and therefore forestFile).
var _ walTarget = (*mmapFile)(nil)

// mmapFile wraps an *os.File for non-unix platforms. It provides the
// same dirty-buffer + SeekEnd behavior as the unix mmap version but
// uses normal file I/O for reads that miss the dirty buffer.
type mmapFile struct {
	file *os.File
	pos  int64

	// Dirty write buffer — same as unix version.
	dirty      cacheStore
	maxWritten int64
	baseSize   int64
}

func newMmapFile(f *os.File, maxSize int64, entrySize int, maxCacheBytes int64) (*mmapFile, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("mmapFile stat: %w", err)
	}
	logicalSize := fi.Size()

	if maxCacheBytes <= 0 {
		maxCacheBytes = defaultMaxCacheMemory
	}

	dirty, err := newCacheStore(entrySize, maxCacheBytes)
	if err != nil {
		return nil, err
	}

	return &mmapFile{
		file:       f,
		dirty:      dirty,
		maxWritten: logicalSize,
		baseSize:   logicalSize,
	}, nil
}

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
	return m.file.ReadAt(p, off)
}

func (m *mmapFile) Read(p []byte) (int, error) {
	if cached, ok := m.dirty.get(m.pos); ok {
		n := copy(p, cached)
		m.pos += int64(n)
		return n, nil
	}
	if m.pos >= m.baseSize {
		for i := range p {
			p[i] = 0
		}
		n := len(p)
		m.pos += int64(n)
		return n, nil
	}
	if _, err := m.file.Seek(m.pos, io.SeekStart); err != nil {
		return 0, err
	}
	n, err := m.file.Read(p)
	m.pos += int64(n)
	return n, err
}

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
	if err := m.file.Truncate(size); err != nil {
		return err
	}
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

func (m *mmapFile) forEachDirty(fn func(offset int64, data []byte)) {
	m.dirty.forEach(fn)
}

func (m *mmapFile) dirtyCount() int     { return m.dirty.count() }
func (m *mmapFile) dirtyEntrySize() int { return m.dirty.entrySize() }
func (m *mmapFile) flushNeeded() bool   { return m.dirty.overflowed() }

func (m *mmapFile) applyDirty() error {
	var applyErr error
	m.dirty.forEach(func(offset int64, data []byte) {
		if applyErr != nil {
			return
		}
		if _, err := m.file.Seek(offset, io.SeekStart); err != nil {
			applyErr = err
			return
		}
		if _, err := m.file.Write(data); err != nil {
			applyErr = err
			return
		}
	})
	return applyErr
}

func (m *mmapFile) syncUnderlying() error {
	return m.file.Sync()
}

func (m *mmapFile) resetAfterFlush() error {
	m.dirty.clear()
	m.baseSize = m.maxWritten
	m.pos = 0
	return nil
}

func (m *mmapFile) discard() {
	m.dirty.clear()
	m.maxWritten = m.baseSize
	m.pos = 0
}

func (m *mmapFile) Close() error {
	return m.file.Close()
}

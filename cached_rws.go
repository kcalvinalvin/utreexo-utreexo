package utreexo

import (
	"fmt"
	"io"
)

// cachedRWS wraps an io.ReadWriteSeeker, buffering all writes in memory.
// Reads check the buffer first (exact offset match), then fall through to
// the underlying file on miss. This enables atomic modifications: call
// Flush to commit buffered writes, or Discard to throw them away.
//
// cachedRWS assumes all I/O is aligned, fixed-size records. Reads and
// writes at offset X always use the same record size. Partial overlap
// between a cached write and a subsequent read at a different offset is
// NOT handled -- the read will miss the cache and return stale data from
// the underlying file.
type cachedRWS struct {
	underlying forestFile
	cache      cacheStore
	pos        int64 // current seek position
	maxWritten int64 // highest byte offset written (for SeekEnd)
	baseSize   int64 // underlying file size at wrap time
}

// newCachedRWS creates a cachedRWS wrapping the given io.ReadWriteSeeker.
// It seeks to the end of the underlying file to determine its current size,
// which is needed for correct SeekEnd behavior (e.g. append patterns).
// entrySize specifies the fixed record size (4, 8, or 32 bytes).
// maxCacheBytes sets the threshold for signaling that a flush is needed.
// If maxCacheBytes is 0, defaultMaxCacheMemory is used.
func newCachedRWS(underlying forestFile, entrySize int, maxCacheBytes int64) (*cachedRWS, error) {
	size, err := underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if maxCacheBytes <= 0 {
		maxCacheBytes = defaultMaxCacheMemory
	}

	cache, err := newCacheStore(entrySize, maxCacheBytes)
	if err != nil {
		return nil, err
	}

	return &cachedRWS{
		underlying: underlying,
		cache:      cache,
		pos:        size,
		maxWritten: size,
		baseSize:   size,
	}, nil
}

// ReadAt reads len(p) bytes starting at byte offset off.
// It checks the cache first, then falls through to the underlying file.
func (c *cachedRWS) ReadAt(p []byte, off int64) (int, error) {
	if c.cache.copyTo(off, p) {
		return len(p), nil
	}
	if off >= c.baseSize {
		return 0, io.EOF
	}
	return c.underlying.ReadAt(p, off)
}

// Read returns data from the cache if present, otherwise reads from
// the underlying file. Positions beyond the underlying file size
// return zeros without touching the file.
func (c *cachedRWS) Read(p []byte) (int, error) {
	if c.cache.copyTo(c.pos, p) {
		n := len(p)
		c.pos += int64(n)
		return n, nil
	}
	// If the read is entirely beyond the underlying file, return zeros
	// without touching it. This avoids extending an in-memory file (or
	// hitting EOF on a real file) for positions that only exist in the cache.
	if c.pos >= c.baseSize {
		for i := range p {
			p[i] = 0
		}
		n := len(p)
		c.pos += int64(n)
		return n, nil
	}
	// Fall through to underlying file.
	_, err := c.underlying.Seek(c.pos, io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := c.underlying.Read(p)
	c.pos += int64(n)
	return n, err
}

// Write stores data in the cache at the current position. The data length
// must match the cache's entry size (4, 8, or 32 bytes).
func (c *cachedRWS) Write(p []byte) (int, error) {
	switch cache := c.cache.(type) {
	case *cacheMap4:
		if len(p) != 4 {
			return 0, fmt.Errorf("expected 4 bytes, got %d", len(p))
		}
		cache.put4(c.pos, [4]byte(p))
	case *cacheMap8:
		if len(p) != 8 {
			return 0, fmt.Errorf("expected 8 bytes, got %d", len(p))
		}
		cache.put8(c.pos, [8]byte(p))
	case *cacheMap32:
		if len(p) != 32 {
			return 0, fmt.Errorf("expected 32 bytes, got %d", len(p))
		}
		cache.put32(c.pos, [32]byte(p))
	default:
		return 0, fmt.Errorf("unsupported cache type %T", c.cache)
	}

	n := len(p)
	c.pos += int64(n)
	if c.pos > c.maxWritten {
		c.maxWritten = c.pos
	}
	return n, nil
}

// WriteAt stores data in the cache at the given offset without changing
// the current seek position. This avoids the overhead of a separate
// Seek + Write pair.
func (c *cachedRWS) WriteAt(p []byte, off int64) (int, error) {
	switch cache := c.cache.(type) {
	case *cacheMap4:
		if len(p) != 4 {
			return 0, fmt.Errorf("expected 4 bytes, got %d", len(p))
		}
		cache.put4(off, [4]byte(p))
	case *cacheMap8:
		if len(p) != 8 {
			return 0, fmt.Errorf("expected 8 bytes, got %d", len(p))
		}
		cache.put8(off, [8]byte(p))
	case *cacheMap32:
		if len(p) != 32 {
			return 0, fmt.Errorf("expected 32 bytes, got %d", len(p))
		}
		cache.put32(off, [32]byte(p))
	default:
		return 0, fmt.Errorf("unsupported cache type %T", c.cache)
	}

	n := len(p)
	end := off + int64(n)
	if end > c.maxWritten {
		c.maxWritten = end
	}
	return n, nil
}

// Seek sets the position for the next Read or Write. For SeekEnd,
// the end is defined as maxWritten (the highest position written to).
func (c *cachedRWS) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		c.pos = offset
	case io.SeekCurrent:
		c.pos += offset
	case io.SeekEnd:
		c.pos = c.maxWritten + offset
	default:
		return 0, fmt.Errorf("cachedRWS: invalid whence %d", whence)
	}
	return c.pos, nil
}

// Flush writes all cached data to the underlying file and clears the cache.
func (c *cachedRWS) Flush() error {
	var flushErr error
	c.cache.forEach(func(offset int64, data []byte) {
		if flushErr != nil {
			return
		}
		_, err := c.underlying.Seek(offset, io.SeekStart)
		if err != nil {
			flushErr = err
			return
		}
		_, err = c.underlying.Write(data)
		if err != nil {
			flushErr = err
			return
		}
	})
	if flushErr != nil {
		return flushErr
	}
	c.cache.clear()

	// Update baseSize so subsequent reads can reach the newly flushed data
	// in the underlying file.
	size, err := c.underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	c.baseSize = size
	return nil
}

// Discard drops all buffered writes without touching the underlying file.
func (c *cachedRWS) Discard() {
	c.cache.clear()
	c.maxWritten = c.baseSize
	c.pos = 0
}

// FlushNeeded returns true if the cache has exceeded its memory threshold
// (i.e., entries have spilled into the overflow map).
func (c *cachedRWS) FlushNeeded() bool {
	return c.cache.overflowed()
}

// resetAfterFlush clears the cache and updates baseSize/maxWritten to
// reflect the current underlying file size. This should be called after
// writes have been applied to the underlying file (e.g. by the WAL),
// so that a subsequent Discard resets to the correct post-flush baseline.
func (c *cachedRWS) resetAfterFlush() error {
	c.cache.clear()
	size, err := c.underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	c.baseSize = size
	c.maxWritten = size
	c.pos = 0
	return nil
}

// Assert that cachedRWS implements walTarget.
var _ walTarget = (*cachedRWS)(nil)

// forEachDirty iterates over all dirty entries in the cache.
func (c *cachedRWS) forEachDirty(fn func(offset int64, data []byte)) {
	c.cache.forEach(fn)
}

// dirtyCount returns the number of dirty entries.
func (c *cachedRWS) dirtyCount() int {
	return c.cache.count()
}

// dirtyEntrySize returns the fixed entry size.
func (c *cachedRWS) dirtyEntrySize() int {
	return c.cache.entrySize()
}

// flushNeeded returns true if the cache has exceeded its memory threshold.
func (c *cachedRWS) flushNeeded() bool {
	return c.cache.overflowed()
}

// applyDirty writes all cached entries to the underlying file without
// clearing the cache.
func (c *cachedRWS) applyDirty() error {
	var applyErr error
	c.cache.forEach(func(offset int64, data []byte) {
		if applyErr != nil {
			return
		}
		if _, err := c.underlying.Seek(offset, io.SeekStart); err != nil {
			applyErr = err
			return
		}
		if _, err := c.underlying.Write(data); err != nil {
			applyErr = err
			return
		}
	})
	return applyErr
}

// syncUnderlying fsyncs the underlying file.
func (c *cachedRWS) syncUnderlying() error {
	return syncFile(c.underlying)
}

// discard drops all buffered writes without touching the underlying file.
func (c *cachedRWS) discard() {
	c.Discard()
}

// Truncate truncates the underlying file to the specified size.
// It also invalidates any cached writes beyond the new size and updates
// the internal size tracking.
func (c *cachedRWS) Truncate(size int64) error {
	truncater, ok := c.underlying.(interface{ Truncate(int64) error })
	if !ok {
		return fmt.Errorf("underlying file does not support Truncate")
	}
	if err := truncater.Truncate(size); err != nil {
		return err
	}

	// Invalidate cached writes beyond the new size.
	c.cache.deleteAbove(size)

	// Update size tracking.
	c.baseSize = size
	if c.maxWritten > size {
		c.maxWritten = size
	}
	if c.pos > size {
		c.pos = size
	}

	return nil
}

//go:build unix

package utreexo

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// Verify mmapFile satisfies walTarget (and therefore forestFile).
var _ walTarget = (*mmapFile)(nil)

func TestMmapFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "mmaptest-*")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()

	const maxSize = 4096

	m, err := newMmapFile(f, maxSize, 32, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Initial logicalSize should be 0.
	pos, err := m.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Fatalf("initial logicalSize: got %d, want 0", pos)
	}

	// Write a 32-byte entry.
	var data [32]byte
	copy(data[:], "hello, mmap! entry number one")
	if _, err := m.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	n, err := m.Write(data[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 32 {
		t.Fatalf("Write: got %d bytes, want 32", n)
	}

	// maxWritten should match written data.
	pos, err = m.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 32 {
		t.Fatalf("logicalSize after write: got %d, want 32", pos)
	}

	// Read back via Read (dirty buffer hit).
	if _, err := m.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	_, err = m.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[:]) {
		t.Fatalf("Read: got %x, want %x", buf, data[:])
	}

	// ReadAt from dirty buffer.
	buf2 := make([]byte, 32)
	_, err = m.ReadAt(buf2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf2, data[:]) {
		t.Fatalf("ReadAt: got %x, want %x", buf2, data[:])
	}

	// ReadAt beyond baseSize returns EOF (data not yet flushed).
	_, err = m.ReadAt(buf2, 32)
	if err != io.EOF {
		t.Fatalf("ReadAt beyond baseSize: got %v, want EOF", err)
	}

	// Write at a gap (entry at offset 192, skipping over offsets 32-191).
	if _, err := m.Seek(192, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var gapData [32]byte
	copy(gapData[:], "sparse entry at offset 192")
	if _, err := m.Write(gapData[:]); err != nil {
		t.Fatal(err)
	}

	// Gap should be zeros (read at offset 96 misses dirty buffer, reads zeros from mmap).
	gapBuf := make([]byte, 32)
	if _, err := m.Seek(96, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	_, err = m.Read(gapBuf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gapBuf, make([]byte, 32)) {
		t.Fatalf("gap read: got %x, want zeros", gapBuf)
	}

	// Apply dirty to mmap and sync.
	if err := m.applyDirty(); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := m.resetAfterFlush(); err != nil {
		t.Fatal(err)
	}

	// Close and reopen.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	f2, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := newMmapFile(f2, maxSize, 32, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	// maxWritten preserved across close/reopen.
	pos, err = m2.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(192 + 32)
	if pos != want {
		t.Fatalf("logicalSize after reopen: got %d, want %d", pos, want)
	}

	// Original data intact (reads directly from mmap now).
	buf3 := make([]byte, 32)
	if _, err := m2.ReadAt(buf3, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf3, data[:]) {
		t.Fatalf("after reopen: got %x, want %x", buf3, data[:])
	}

	// Gap data intact.
	buf4 := make([]byte, 32)
	if _, err := m2.ReadAt(buf4, 192); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf4, gapData[:]) {
		t.Fatalf("after reopen gap: got %x, want %x", buf4, gapData[:])
	}

	// Truncate.
	if err := m2.Truncate(50); err != nil {
		t.Fatal(err)
	}
	pos, err = m2.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 50 {
		t.Fatalf("logicalSize after truncate: got %d, want 50", pos)
	}

	// Read beyond truncated baseSize returns EOF.
	_, err = m2.ReadAt(buf4, 192)
	if err != io.EOF {
		t.Fatalf("ReadAt after truncate: got %v, want EOF", err)
	}
}

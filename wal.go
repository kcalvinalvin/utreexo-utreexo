package utreexo

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// wal coordinates crash-safe writes across walTarget instances and a
// deletedBitmap. It uses a write-ahead journal to ensure atomicity:
// either all buffered writes are applied to the underlying files, or
// none are.
//
// Journal format (all little-endian):
//
//	totalLen  uint64   (byte length of entries block)
//	bestHash  [32]byte (consistency hash, e.g. best block hash)
//	[entries] ...      (totalLen bytes)
//	checksum  uint32   (CRC32-IEEE of totalLen + bestHash + entries)
//
// Each entry:
//
//	fileIdx   uint8    (which underlying file)
//	offset    int64    (byte offset in that file)
//	dataLen   uint32   (length of data)
//	data      []byte   (the actual bytes)
//
// File indices in the journal:
//
//	0 = main hash file     (walTarget)
//	1 = addIndex file      (walTarget)
//	2 = meta file          (walTarget)
//	3 = deleted bitmap file (dirty words from deletedBitmap)
//
// Flush sequence:
//  1. Write bestHash into meta target's dirty buffer
//  2. Serialize entries from walTarget dirty buffers + dirty bitmap words
//  3. Write bestHash + entries + CRC32 checksum to journal
//  4. Sync journal
//  5. Apply dirty entries from walTargets to underlying storage
//  6. Apply dirty bitmap words to bitmap file
//  7. Sync underlying files
//  8. Clear journal (write totalLen=0, sync)
//  9. Reset walTarget dirty buffers + clear bitmap dirty tracking
//
// Recovery (in newWAL):
//  1. Read journal; if valid checksum found, replay entries through walTargets
//  2. Apply dirty buffers to underlying storage, sync
//  3. Clear journal
//  4. Load bitmap from recovered underlying file
//
// The consistency hash is written to file 2 (metaFile) at offset 32,
// and can be read from there after recovery or normal startup.
type wal struct {
	journal    io.ReadWriteSeeker
	targets    [3]walTarget // [0]=main, [1]=addIndex, [2]=meta
	bitmap     *deletedBitmap
	bitmapFile forestFile
	onFlush    func([32]byte) error
}

// journalEntry represents a single write operation in the WAL journal.
type journalEntry struct {
	fileIdx uint8
	offset  int64
	data    []byte
}

const (
	journalHeaderSize   = 8  // uint64 totalLen
	journalHashSize     = 32 // [32]byte bestHash
	journalChecksumSize = 4  // uint32 CRC32
	entryHeaderSize     = 13 // 1 (fileIdx) + 8 (offset) + 4 (dataLen)
	journalMinSize      = journalHeaderSize + journalHashSize + journalChecksumSize

	metaFileIdx    = 2  // journal fileIdx for the metadata file
	deletedFileIdx = 3  // journal fileIdx for the deleted bitmap file
	bestHashOffset = 32 // byte offset of the consistency hash in the metadata file
)

// walTarget is the interface for files managed by the WAL. It combines
// forestFile (for reads/writes) with dirty-buffer tracking for crash-safe
// atomic commits. Both *mmapFile (data, addIndex) and *cachedRWS (meta)
// implement this interface.
type walTarget interface {
	forestFile

	// forEachDirty iterates over all dirty (buffered) entries.
	forEachDirty(fn func(offset int64, data []byte))

	// dirtyCount returns the number of dirty entries.
	dirtyCount() int

	// dirtyEntrySize returns the fixed size of each dirty entry in bytes.
	dirtyEntrySize() int

	// flushNeeded returns true if the dirty buffer has exceeded its
	// memory threshold.
	flushNeeded() bool

	// applyDirty copies all dirty entries into the underlying storage
	// (mmap data[] or underlying file). Does NOT clear the dirty buffer.
	applyDirty() error

	// syncUnderlying fsyncs the underlying storage.
	syncUnderlying() error

	// resetAfterFlush clears the dirty buffer and updates internal size
	// tracking to reflect the current storage state.
	resetAfterFlush() error

	// discard drops all dirty entries without applying them.
	discard()
}

// newWAL creates a wal coordinating writes across the given walTargets.
// bitmapFile is the deleted-positions bitmap (not a walTarget — its dirty
// words are tracked by the in-memory deletedBitmap instead).
// targets must be exactly 3: [0]=main, [1]=addIndex, [2]=meta.
// After recovery the bitmap is loaded from the underlying file and accessible
// via Bitmap(). Use Target(i) to get the walTarget for file i.
func newWAL(journal io.ReadWriteSeeker, bitmapFile forestFile, targets ...walTarget) (*wal, error) {
	if len(targets) != 3 {
		return nil, fmt.Errorf("wal requires exactly 3 targets, got %d", len(targets))
	}

	w := &wal{
		journal:    journal,
		bitmapFile: bitmapFile,
	}
	copy(w.targets[:], targets)

	// Build the underlying file array for journal recovery.
	// Indices 0-2 are walTargets (writes go to dirty buffer),
	// index 3 is the bitmapFile (writes go directly to file).
	underlying := make([]forestFile, 4)
	for i := range 3 {
		underlying[i] = w.targets[i]
	}
	underlying[deletedFileIdx] = bitmapFile

	// Replay any committed journal entries.
	if err := w.recoverFromJournal(underlying); err != nil {
		return nil, fmt.Errorf("wal recover: %w", err)
	}

	// Load bitmap from the (possibly recovered) underlying file.
	bitmap, err := loadDeletedBitmap(w.bitmapFile)
	if err != nil {
		return nil, fmt.Errorf("wal load bitmap: %w", err)
	}
	w.bitmap = bitmap

	return w, nil
}

// Target returns the walTarget for the i-th file.
// Indices: 0=main, 1=addIndex, 2=meta.
func (w *wal) Target(i int) walTarget {
	return w.targets[i]
}

// Bitmap returns the in-memory deleted bitmap loaded from the underlying file.
// The bitmap tracks dirty words which are serialized during Flush.
func (w *wal) Bitmap() *deletedBitmap {
	return w.bitmap
}

// SetOnFlush registers a callback that runs after each successful Flush.
// The callback receives the bestHash that was flushed.
func (w *wal) SetOnFlush(fn func([32]byte) error) {
	w.onFlush = fn
}

// Flush atomically commits all cached writes through the journal.
// The bestHash is written to metaFile (file index 2) at offset 32.
func (w *wal) Flush(bestHash [32]byte) error {
	// Write bestHash into the meta target's dirty buffer so it's included
	// in serialization and applyDirty automatically.
	if _, err := w.targets[metaFileIdx].Seek(bestHashOffset, io.SeekStart); err != nil {
		return fmt.Errorf("wal meta seek: %w", err)
	}
	if _, err := w.targets[metaFileIdx].Write(bestHash[:]); err != nil {
		return fmt.Errorf("wal meta write bestHash: %w", err)
	}

	// Serialize entries directly from dirty buffers.
	entriesBuf := w.serializeEntries()
	totalLen := uint64(len(entriesBuf))

	// Build header.
	var header [journalHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], totalLen)

	// Compute CRC32 over header + bestHash + entries.
	crc := crc32.NewIEEE()
	crc.Write(header[:])
	crc.Write(bestHash[:])
	crc.Write(entriesBuf)
	var checksumBuf [journalChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBuf[:], crc.Sum32())

	// Write journal: header + bestHash + entries + checksum.
	if _, err := w.journal.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal journal seek: %w", err)
	}
	if _, err := w.journal.Write(header[:]); err != nil {
		return fmt.Errorf("wal journal write header: %w", err)
	}
	if _, err := w.journal.Write(bestHash[:]); err != nil {
		return fmt.Errorf("wal journal write bestHash: %w", err)
	}
	if _, err := w.journal.Write(entriesBuf); err != nil {
		return fmt.Errorf("wal journal write entries: %w", err)
	}
	if _, err := w.journal.Write(checksumBuf[:]); err != nil {
		return fmt.Errorf("wal journal write checksum: %w", err)
	}

	// Sync journal to ensure it's durable before touching underlying files.
	if err := syncFile(w.journal); err != nil {
		return fmt.Errorf("wal journal sync: %w", err)
	}

	// Apply dirty entries from targets to underlying storage + bitmap.
	if err := w.applyFromTargets(); err != nil {
		return fmt.Errorf("wal apply: %w", err)
	}

	// Sync underlying files (including bitmap file).
	for i, t := range w.targets {
		if err := t.syncUnderlying(); err != nil {
			return fmt.Errorf("wal sync target %d: %w", i, err)
		}
	}
	if err := syncFile(w.bitmapFile); err != nil {
		return fmt.Errorf("wal sync bitmap file: %w", err)
	}

	// Clear journal.
	if err := w.clearJournal(); err != nil {
		return fmt.Errorf("wal clear journal: %w", err)
	}

	// Reset targets so size tracking reflects the new underlying state.
	for i, t := range w.targets {
		if err := t.resetAfterFlush(); err != nil {
			return fmt.Errorf("wal reset target %d: %w", i, err)
		}
	}

	// Clear bitmap dirty tracking.
	w.bitmap.clearDirty()

	if w.onFlush != nil {
		if err := w.onFlush(bestHash); err != nil {
			return fmt.Errorf("wal onFlush: %w", err)
		}
	}

	return nil
}

// serializeEntries encodes journal entries directly from walTarget dirty
// buffers and the dirty bitmap into a byte slice.
func (w *wal) serializeEntries() []byte {
	// Pre-calculate total size.
	size := 0
	for _, t := range w.targets {
		size += t.dirtyCount() * (entryHeaderSize + t.dirtyEntrySize())
	}
	// Add dirty bitmap entries.
	size += len(w.bitmap.dirtyWords) * (entryHeaderSize + 8)

	buf := make([]byte, 0, size)

	// Serialize entries from each target's dirty buffer.
	for i, t := range w.targets {
		fileIdx := uint8(i)
		t.forEachDirty(func(offset int64, data []byte) {
			buf = append(buf, fileIdx)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(offset))
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
			buf = append(buf, data...)
		})
	}

	// Serialize dirty bitmap words.
	w.bitmap.forEachDirty(func(offset int64, data []byte) {
		buf = append(buf, deletedFileIdx)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(offset))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, data...)
	})

	return buf
}

// applyFromTargets writes dirty entries from walTargets to underlying
// storage and applies dirty bitmap words to the bitmap file.
func (w *wal) applyFromTargets() error {
	for i, t := range w.targets {
		if err := t.applyDirty(); err != nil {
			return fmt.Errorf("apply dirty target %d: %w", i, err)
		}
	}

	// Apply dirty bitmap words to the underlying bitmap file.
	var bitmapErr error
	w.bitmap.forEachDirty(func(offset int64, data []byte) {
		if bitmapErr != nil {
			return
		}
		if _, err := w.bitmapFile.Seek(offset, io.SeekStart); err != nil {
			bitmapErr = fmt.Errorf("bitmap file seek to %d: %w", offset, err)
			return
		}
		if _, err := w.bitmapFile.Write(data); err != nil {
			bitmapErr = fmt.Errorf("bitmap file write at %d: %w", offset, err)
			return
		}
	})
	return bitmapErr
}

// Discard drops all pending writes without committing.
// The in-memory bitmap is reloaded from the underlying file to revert
// any mutations from the discarded block.
func (w *wal) Discard() error {
	for _, t := range w.targets {
		t.discard()
	}
	// Reload bitmap from the underlying file to revert in-memory mutations.
	// Unlike walTargets (which are read-through over the underlying storage),
	// the bitmap is fully in-memory, so we must explicitly restore it.
	bitmap, err := loadDeletedBitmap(w.bitmapFile)
	if err != nil {
		return fmt.Errorf("wal discard: reload bitmap: %w", err)
	}
	w.bitmap = bitmap
	return nil
}

// FlushNeeded returns true if any walTarget has exceeded its memory threshold.
// Dirty bitmap words are not considered here because they are tiny (just word
// indices) and will be written to the journal when a flush is triggered by a
// walTarget overflow.
func (w *wal) FlushNeeded() bool {
	for _, t := range w.targets {
		if t.flushNeeded() {
			return true
		}
	}
	return false
}

// parseEntries decodes journal entries from a byte slice.
func parseEntries(buf []byte) ([]journalEntry, error) {
	var entries []journalEntry
	for len(buf) >= entryHeaderSize {
		fileIdx := buf[0]
		offset := int64(binary.LittleEndian.Uint64(buf[1:9]))
		dataLen := binary.LittleEndian.Uint32(buf[9:entryHeaderSize])
		buf = buf[entryHeaderSize:]

		if uint32(len(buf)) < dataLen {
			return nil, fmt.Errorf("wal: truncated entry data: need %d, have %d", dataLen, len(buf))
		}

		data := make([]byte, dataLen)
		copy(data, buf[:dataLen])
		buf = buf[dataLen:]

		entries = append(entries, journalEntry{
			fileIdx: fileIdx,
			offset:  offset,
			data:    data,
		})
	}
	if len(buf) != 0 {
		return nil, fmt.Errorf("wal: %d trailing bytes after last entry", len(buf))
	}
	return entries, nil
}

// applyEntries writes each entry to the appropriate underlying file.
// For walTargets (indices 0-2), writes go to the dirty buffer; for the
// bitmap file (index 3), writes go directly to the file.
func applyEntries(entries []journalEntry, underlying []forestFile) error {
	for _, e := range entries {
		if int(e.fileIdx) >= len(underlying) {
			return fmt.Errorf("wal: fileIdx %d out of range (have %d files)", e.fileIdx, len(underlying))
		}
		f := underlying[e.fileIdx]
		if _, err := f.Seek(e.offset, io.SeekStart); err != nil {
			return err
		}
		if _, err := f.Write(e.data); err != nil {
			return err
		}
	}
	return nil
}

// clearJournal writes a zero totalLen to indicate no pending transaction
// and truncates the file to reclaim disk space.
func (w *wal) clearJournal() error {
	if _, err := w.journal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var zero [journalHeaderSize]byte
	if _, err := w.journal.Write(zero[:]); err != nil {
		return err
	}
	// Truncate to reclaim disk space if the file supports it.
	if truncater, ok := w.journal.(interface{ Truncate(size int64) error }); ok {
		if err := truncater.Truncate(journalHeaderSize); err != nil {
			return err
		}
	}
	return syncFile(w.journal)
}

// recoverFromJournal replays any committed journal entries to the
// underlying files. Called once during newWAL.
//
// For walTargets (indices 0-2), recovered entries go through the dirty
// buffer and are then applied via applyDirty + syncUnderlying + resetAfterFlush.
// For the bitmap file (index 3), entries are written directly to the file.
func (w *wal) recoverFromJournal(underlying []forestFile) error {
	// Get journal size.
	size, err := w.journal.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if size < int64(journalMinSize) {
		return nil // No valid journal.
	}

	// Read totalLen.
	if _, err := w.journal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var totalLen uint64
	if err := binary.Read(w.journal, binary.LittleEndian, &totalLen); err != nil {
		return nil // Can't read header, nothing to recover.
	}

	if totalLen == 0 {
		return nil // No pending transaction.
	}

	// Bound totalLen to the file size and int64 limits to avoid overflow/OOM.
	if totalLen > uint64(size)-journalMinSize {
		return w.clearJournal()
	}
	if totalLen > ^uint64(0)>>1 { // max int64
		return w.clearJournal()
	}

	// Check if the full record fits in the file.
	recordSize := int64(journalHeaderSize) + int64(journalHashSize) + int64(totalLen) + int64(journalChecksumSize)
	if recordSize > size {
		// Incomplete write; underlying files have the old consistent state.
		return w.clearJournal()
	}

	// Read bestHash.
	var bestHash [journalHashSize]byte
	if _, err := io.ReadFull(w.journal, bestHash[:]); err != nil {
		return w.clearJournal()
	}

	// Read entries.
	entriesBuf := make([]byte, totalLen)
	if _, err := io.ReadFull(w.journal, entriesBuf); err != nil {
		return w.clearJournal()
	}

	// Read stored checksum.
	var storedChecksum uint32
	if err := binary.Read(w.journal, binary.LittleEndian, &storedChecksum); err != nil {
		return w.clearJournal()
	}

	// Verify CRC32 over header + bestHash + entries.
	crc := crc32.NewIEEE()
	var header [journalHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], totalLen)
	crc.Write(header[:])
	crc.Write(bestHash[:])
	crc.Write(entriesBuf)

	if crc.Sum32() != storedChecksum {
		// Corrupt journal; discard.
		return w.clearJournal()
	}

	// Parse and replay entries.
	entries, err := parseEntries(entriesBuf)
	if err != nil {
		return w.clearJournal()
	}

	if err := applyEntries(entries, underlying); err != nil {
		return err
	}

	// For walTargets (indices 0-2), the entries went into the dirty buffer.
	// Apply them to the actual underlying storage and sync.
	for _, t := range w.targets {
		if err := t.applyDirty(); err != nil {
			return err
		}
		if err := t.syncUnderlying(); err != nil {
			return err
		}
		if err := t.resetAfterFlush(); err != nil {
			return err
		}
	}

	// Sync the bitmap file (entries went directly to it).
	if err := syncFile(w.bitmapFile); err != nil {
		return err
	}

	return w.clearJournal()
}

// syncFile calls Sync() on f if it supports it (e.g. *os.File).
// For implementations without Sync (e.g. memFile), this is a no-op.
func syncFile(f io.ReadWriteSeeker) error {
	if syncer, ok := f.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	headerSize = 16
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

type LogEntry struct {
	Data  []byte
	Index uint64
	CRC32 uint32
}

type WAL struct {
	mu          sync.RWMutex
	activeFile  *os.File
	buffer      []*LogEntry
	batchSize   int
	segmentDir  string
	segmentSize int64
	nextIndex   uint64
	notifyChan  chan struct{}
	closeChan   chan struct{}
	closeOnce   sync.Once
	logger      *zap.Logger
	segments    []*SegmentMeta
}

type SegmentMeta struct {
	StartIndex uint64
	EndIndex   uint64
	FilePath   string
	IsSealed   bool
}

const (
	DefaultSegmentSize = 512 * 1024 * 1024
	DefaultBatchSize   = 1000
)

func NewWAL(segmentDir string) (*WAL, error) {
	if err := os.MkdirAll(segmentDir, 0o755); err != nil {
		return nil, fmt.Errorf("create segment dir failed: %w", err)
	}

	wal := &WAL{
		segmentDir:  segmentDir,
		batchSize:   DefaultBatchSize,
		segmentSize: DefaultSegmentSize,
		notifyChan:  make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
		logger:      zap.NewNop(),
		segments:    make([]*SegmentMeta, 0),
	}

	if err := wal.recoverSegments(); err != nil {
		return nil, err
	}

	go wal.backgroundFlush()

	return wal, nil
}

func (w *WAL) Append(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Index = w.nextIndex
	entry.CRC32 = crc32.Checksum(entry.Data, castagnoli)
	w.nextIndex++
	w.buffer = append(w.buffer, entry)

	if len(w.buffer) >= w.batchSize {
		select {
		case w.notifyChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func (w *WAL) Flush() error {
	return w.flush()
}

func (w *WAL) backgroundFlush() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = w.flush()
		case <-w.notifyChan:
			_ = w.flush()
		case <-w.closeChan:
			return
		}
	}
}

func (w *WAL) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buffer) == 0 {
		return nil
	}
	if w.activeFile == nil {
		return fmt.Errorf("active segment file is not initialized")
	}

	buf := make([]byte, 0, len(w.buffer)*128)
	for _, entry := range w.buffer {
		head := make([]byte, headerSize)
		binary.BigEndian.PutUint32(head[0:4], uint32(len(entry.Data)))
		binary.BigEndian.PutUint64(head[4:12], entry.Index)
		binary.BigEndian.PutUint32(head[12:16], entry.CRC32)
		buf = append(buf, head...)
		buf = append(buf, entry.Data...)
	}

	if _, err := w.activeFile.Write(buf); err != nil {
		return fmt.Errorf("write WAL failed: %w", err)
	}
	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}

	if len(w.segments) > 0 {
		w.segments[len(w.segments)-1].EndIndex = w.buffer[len(w.buffer)-1].Index
	}

	if stat, err := w.activeFile.Stat(); err == nil && stat.Size() >= w.segmentSize {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	w.buffer = w.buffer[:0]
	return nil
}

func (w *WAL) Read(fromIndex uint64) ([]*LogEntry, error) {
	if err := w.Flush(); err != nil {
		return nil, err
	}

	w.mu.RLock()
	segments := w.listSegments()
	w.mu.RUnlock()

	var entries []*LogEntry

	for _, seg := range segments {
		if seg.EndIndex != 0 && seg.EndIndex < fromIndex {
			continue
		}

		data, err := os.ReadFile(seg.FilePath)
		if err != nil {
			return nil, err
		}

		pos := 0
		for pos < len(data) {
			if pos+headerSize > len(data) {
				break
			}

			dataLen := binary.BigEndian.Uint32(data[pos : pos+4])
			index := binary.BigEndian.Uint64(data[pos+4 : pos+12])
			storedCRC := binary.BigEndian.Uint32(data[pos+12 : pos+16])
			nextPos := pos + headerSize + int(dataLen)
			if nextPos > len(data) {
				break
			}

			if index >= fromIndex {
				entryData := make([]byte, dataLen)
				copy(entryData, data[pos+headerSize:nextPos])
				actualCRC := crc32.Checksum(entryData, castagnoli)
				if actualCRC != storedCRC {
					return nil, fmt.Errorf("CRC mismatch at index %d", index)
				}
				entries = append(entries, &LogEntry{
					Index: index,
					Data:  entryData,
					CRC32: actualCRC,
				})
			}

			pos = nextPos
		}
	}

	return entries, nil
}

func (w *WAL) recoverSegments() error {
	entries, err := os.ReadDir(w.segmentDir)
	if err != nil {
		return fmt.Errorf("read segment dir failed: %w", err)
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return compareStrings(filepath.Base(a.Name()), filepath.Base(b.Name()))
	})

	var highestIndex uint64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".wal" {
			continue
		}

		filePath := filepath.Join(w.segmentDir, entry.Name())
		endIndex, err := scanSegmentEndIndex(filePath)
		if err != nil {
			return err
		}

		w.segments = append(w.segments, &SegmentMeta{
			FilePath: filePath,
			EndIndex: endIndex,
			IsSealed: true,
		})

		if endIndex >= highestIndex {
			highestIndex = endIndex + 1
		}
	}

	w.nextIndex = highestIndex
	return w.rotateSegment()
}

func (w *WAL) listSegments() []*SegmentMeta {
	segments := make([]*SegmentMeta, len(w.segments))
	copy(segments, w.segments)
	return segments
}

func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.closeChan)
		if w.activeFile != nil {
			err = w.activeFile.Close()
		}
	})
	return err
}

func scanSegmentEndIndex(filePath string) (uint64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	var endIndex uint64
	pos := 0
	for pos < len(data) {
		if pos+headerSize > len(data) {
			break
		}

		dataLen := binary.BigEndian.Uint32(data[pos : pos+4])
		index := binary.BigEndian.Uint64(data[pos+4 : pos+12])
		nextPos := pos + headerSize + int(dataLen)
		if nextPos > len(data) {
			break
		}
		endIndex = index
		pos = nextPos
	}

	return endIndex, nil
}

func compareStrings(left string, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

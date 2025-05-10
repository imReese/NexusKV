// pkg/wal/segment.go
package wal

import (
	"fmt"
	"os"
	"time"
)

// rotateSegment 滚动创建新分片
func (w *WAL) rotateSegment() error {
	// 关闭当前分片
	if err := w.activeFile.Close(); err != nil {
		return err
	}

	// 创建新分片文件
	newPath := filepath.Join(w.segmentDir, 
		fmt.Sprintf("segment_%d_%d.wal", w.nextIndex, time.Now().UnixNano()))
	
	file, err := os.OpenFile(newPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.activeFile = file
	return nil
}
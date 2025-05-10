// pkg/wal/wal_test.go
package wal

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// 并发写入测试（包含数据校验）
func TestWAL_ConcurrentAppend(t *testing.T) {
	// 初始化测试环境
	testDir := "/tmp/wal_concurrent_test"
	defer os.RemoveAll(testDir) // 测试后清理

	wal, err := NewWAL(testDir)
	if err != nil {
		t.Fatalf("初始化 WAL 失败: %v", err)
	}
	defer wal.Close()

	// 并发参数
	workerCount := 100
	entriesPerWorker := 20

	var wg sync.WaitGroup
	wg.Add(workerCount)

	// 启动并发写入
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < entriesPerWorker; j++ {
				data := []byte(fmt.Sprintf("worker-%d-entry-%d", workerID, j))
				entry := &LogEntry{Data: data}

				if err := wal.Append(entry); err != nil {
					t.Errorf("写入失败: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证总条目数
	entries, err := wal.Read(0)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}

	expectedCount := workerCount * entriesPerWorker
	if len(entries) != expectedCount {
		t.Fatalf("条目数量不符，预期 %d，实际 %d", 
			expectedCount, len(entries))
	}

	// 验证数据完整性
	seenEntries := make(map[uint64]bool)
	for _, entry := range entries {
		if seenEntries[entry.Index] {
			t.Fatalf("发现重复索引 %d", entry.Index)
		}
		seenEntries[entry.Index] = true

		expectedCRC := crc32.ChecksumIEEE(entry.Data)
		if entry.CRC32 != expectedCRC {
			t.Fatalf("CRC 校验失败 (索引 %d)", entry.Index)
		}
	}
}

// 崩溃恢复测试（模拟未刷盘数据丢失）
func TestWAL_CrashRecovery(t *testing.T) {
	testDir := "/tmp/wal_crash_test"
	defer os.RemoveAll(testDir)

	// 第一次写入（不刷盘）
	wal1, err := NewWAL(testDir)
	if err != nil {
		t.Fatal(err)
	}

	// 写入条目但不主动刷盘
	entry1 := &LogEntry{Data: []byte("unflushed-data")}
	if err := wal1.Append(entry1); err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃（直接关闭，不调用 Flush）
	wal1.Close()

	// 重新初始化 WAL（应触发恢复）
	wal2, err := NewWAL(testDir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	// 验证未刷盘数据不应存在
	entries, err := wal2.Read(0)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatalf("检测到未刷盘数据残留，数量 %d", len(entries))
	}

	// 写入新条目并正常刷盘
	entry2 := &LogEntry{Data: []byte("flushed-data")}
	if err := wal2.Append(entry2); err != nil {
		t.Fatal(err)
	}
	wal2.Flush()

	// 再次崩溃后恢复
	wal2.Close()
	wal3, _ := NewWAL(testDir)
	defer wal3.Close()

	entries, _ = wal3.Read(0)
	if len(entries) != 1 || string(entries[0].Data) != "flushed-data" {
		t.Fatal("已刷盘数据恢复失败")
	}
}
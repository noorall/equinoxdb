/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package executor

import (
	"bufio"
	"context"
	"encoding/csv"
	"equinox/storage"
	"equinox/storage/types"
	"equinox/web/common"
	"fmt"
	"gorm.io/gorm"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func writeChunk(ctx context.Context, filename string, start, end int64, wg *sync.WaitGroup, id int, e *storage.Engine, totalWritten *atomic.Int64) {
	defer wg.Done()

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Seek 到分配的起始位置
	file.Seek(start, io.SeekStart)
	reader := bufio.NewReader(file)

	// 如果不是第一个 chunk，跳过第一行（可能是半行）
	if start != 0 {
		_, _ = reader.ReadString('\n') // 扔掉半行
	}

	fields := types.Fields{}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			currentOffset, _ := file.Seek(0, io.SeekCurrent)
			if currentOffset >= end {
				return
			}

			line, err := reader.ReadString('\n')
			totalWritten.Add(int64(len(line)))
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Println("读取失败:", err)
				return
			}

			csvReader := csv.NewReader(strings.NewReader(line))
			record, err := csvReader.Read()
			if err != nil {
				fmt.Printf("[Worker %d] CSV解析失败: %v\n", id, err)
				continue
			}
			t, _ := strconv.ParseInt(record[0], 10, 64)
			for i := 1; i < len(record); i++ {
				fields[fmt.Sprintf("field%d", i)] = record[i]
			}
			p, _ := types.NewPoint("test"+strconv.Itoa(id), fields, time.Unix(0, t), types.LifeCycle(id))
			_ = e.Write(p)
		}
	}
}

func RunMultiWriteTaskWithEngine(ctx context.Context, task *common.Task, db *gorm.DB, e *storage.Engine) {
	info, _ := os.Stat(task.DataPath)
	fileSize := info.Size()

	chunkSize := fileSize / int64(task.Thread)

	s := time.Now()
	totalWritten := atomic.Int64{}

	var wg sync.WaitGroup
	for i := 0; i < task.Thread; i++ {
		start := int64(i) * chunkSize
		var end int64
		if i == task.Thread-1 {
			end = fileSize
		} else {
			end = int64(i+1) * chunkSize
		}
		wg.Add(1)
		go writeChunk(ctx, task.DataPath, start, end, &wg, i, e, &totalWritten)
	}

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				task.Progress = int(float64(totalWritten.Load()) / float64(fileSize) * 100)
				db.Save(task)
			case <-stop:
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	err := db.First(task, task.ID).Error
	if err == nil {
		task.WriteDuration = time.Since(s).Seconds()
		if !task.Stopped {
			task.Progress = 100
			task.Finished = true
		}
		task.WrittenSize = e.GetWrittenSize()
		task.WrittenTotal = e.GetTotalWritten()
		task.FinishedAt = time.Now()
		db.Save(task)
	}
}

func RunMultiWriteTask(ctx context.Context, task *common.Task, db *gorm.DB) {
	var e *storage.Engine
	if task.Type == 0 {
		e = getTestDBNonSep()
	} else {
		e = getTestDB()
	}
	_ = e.Open(context.Background())
	RunMultiWriteTaskWithEngine(ctx, task, db, e)
	_ = e.Close()
}

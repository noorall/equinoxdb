package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"equinox/storage"
	"equinox/storage/types"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const workerCount = 1 // 并发数

func writeChunk(filename string, start, end int64, wg *sync.WaitGroup, id int, e *storage.Engine) {
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

	for {
		// 当前偏移
		currentOffset, _ := file.Seek(0, io.SeekCurrent)
		if currentOffset >= end {
			break
		}

		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("读取失败:", err)
			break
		}

		// 解析 CSV
		csvReader := csv.NewReader(strings.NewReader(line))
		record, err := csvReader.Read()
		if err != nil {
			fmt.Printf("[Worker %d] CSV解析失败: %v\n", id, err)
			continue
		}
		fields := types.Fields{
			"field" + strconv.Itoa(id): record[1],
		}
		t, _ := strconv.ParseInt(record[0], 10, 64)
		p, _ := types.NewPoint("hh", fields, time.Unix(0, t), types.LifeCycle(id))
		e.Write(p)
	}
}

func runMultiEnableSeparator() {
	e := getTestDB()
	_ = e.Open(context.Background())
	defer e.Close()

	info, _ := os.Stat(DataPath)
	fileSize := info.Size()

	chunkSize := fileSize / int64(workerCount)

	start := time.Now()
	ts := float64(0)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		start := int64(i) * chunkSize
		var end int64
		if i == workerCount-1 {
			end = fileSize
		} else {
			end = int64(i+1) * chunkSize
		}
		wg.Add(1)
		go writeChunk(DataPath, start, end, &wg, i, e)
	}
	wg.Wait()
	ts += time.Since(start).Seconds()
	fmt.Printf("%f MB/s \n", 10240/ts)
}

func runMultiDisableSeparator() {
	e := getTestDBNonSep()
	_ = e.Open(context.Background())
	defer e.Close()

	info, _ := os.Stat(DataPath)
	fileSize := info.Size()

	chunkSize := fileSize / int64(workerCount)

	start := time.Now()
	ts := float64(0)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		start := int64(i) * chunkSize
		var end int64
		if i == workerCount-1 {
			end = fileSize
		} else {
			end = int64(i+1) * chunkSize
		}
		wg.Add(1)
		go writeChunk(DataPath, start, end, &wg, i, e)
	}
	wg.Wait()
	ts += time.Since(start).Seconds()
	fmt.Printf("%f MB/s \n", 10240/ts)
}

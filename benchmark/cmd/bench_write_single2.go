package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const workerCount = 4 // 并发数

func readChunk(filename string, start, end int64, wg *sync.WaitGroup, id int) {
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

		fmt.Printf("[Worker %d] %v\n", id, record)
	}
}

func main3() {
	info, _ := os.Stat("example.csv")
	fileSize := info.Size()

	chunkSize := fileSize / int64(workerCount)

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
		go readChunk("example.csv", start, end, &wg, i)
	}
	wg.Wait()
}

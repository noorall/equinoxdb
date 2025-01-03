package main

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	// 配置参数
	outputFile := "/Users/noorall/GolandProjects/equinox/benchmark/data/random_data2.csv" // 输出文件名
	stringSize := 1024 * 64                                                               // 每个随机字符串的大小（字节）
	dataCount := 40000                                                                    // 生成的数据行数

	// 创建CSV文件
	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV头部（可选）
	//header := []string{"timestamp_ns", "random_data"}
	//if err := writer.Write(header); err != nil {
	//	panic(err)
	//}

	// 生成随机数据
	for i := 0; i < dataCount; i++ {
		// 1. 生成当前纳秒时间戳
		timestamp := time.Now().UnixNano()

		// 2. 生成完全随机的字符串（使用加密安全的随机数生成器）
		randomBytes := make([]byte, stringSize)
		if _, err := rand.Read(randomBytes); err != nil {
			panic(err)
		}
		randomString := hex.EncodeToString(randomBytes) // 转换为16进制确保可打印字符

		// 3. 写入CSV行
		record := []string{
			strconv.FormatInt(timestamp, 10),
			randomString,
		}

		if err := writer.Write(record); err != nil {
			panic(err)
		}

		// 打印进度
		if i%100 == 0 {
			fmt.Printf("Generated %d records...\n", i)
		}
	}

	fmt.Printf("Successfully generated %d records to %s\n", dataCount, outputFile)
}

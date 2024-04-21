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

package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type DataPoint struct {
	Time  uint64
	Value interface{}
}

// ValueType 定义支持的 value 类型
type ValueType int

const (
	Bool ValueType = iota
	Int
	Double
	String
)

// generateTimeSeries 生成时序数据集
func generateTimeSeries(size int, valueType ValueType) []DataPoint {
	data := make([]DataPoint, size)
	rand.Seed(time.Now().UnixNano()) // 设置随机种子

	// 初始时间戳（当前时间）
	startTime := uint64(time.Now().UnixMilli())

	for i := 0; i < size; i++ {
		// 时间戳递增（每条数据间隔 1 秒）
		data[i].Time = startTime + uint64(i*1000)

		// 根据类型生成 value
		switch valueType {
		case Bool:
			data[i].Value = rand.Intn(2) == 1 // 随机生成 true 或 false
		case Int:
			data[i].Value = rand.Intn(100) // 随机生成 0-99 的整数
		case Double:
			data[i].Value = rand.Float64() * 100 // 随机生成 0-100 的浮点数
		case String:
			data[i].Value = fmt.Sprintf("value_%d", i) // 生成字符串 "value_0", "value_1", ...
		}
	}
	return data
}

// writeToCSV 将数据集写入 CSV 文件
func writeToCSV(filename string, data []DataPoint) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	if err := writer.Write([]string{"time", "value"}); err != nil {
		return fmt.Errorf("写入表头失败: %v", err)
	}

	// 写入数据
	for _, dp := range data {
		record := []string{
			strconv.FormatUint(dp.Time, 10), // 时间戳转为字符串
			fmt.Sprintf("%v", dp.Value),     // value 转为字符串
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("写入数据失败: %v", err)
		}
	}
	return nil
}

// readFromCSV 读取 CSV 文件并打印内容
func readFromCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("读取文件失败: %v", err)
	}

	// 打印数据
	for i, record := range records {
		if i == 0 {
			continue // 跳过表头
		}
		fmt.Printf("Time: %s, Value: %s\n", record[0], record[1])
	}
	return nil
}

func main() {
	// 参数：生成 10 条数据，value 类型为 Double
	size := 10
	valueType := Double
	filename := "timeseries.csv"

	// 生成数据集
	data := generateTimeSeries(size, valueType)
	fmt.Println("生成的数据集：")
	for _, dp := range data {
		fmt.Printf("Time: %d, Value: %v\n", dp.Time, dp.Value)
	}

	// 写入 CSV 文件
	if err := writeToCSV(filename, data); err != nil {
		fmt.Println("写入失败:", err)
		return
	}
	fmt.Println("数据已写入文件:", filename)

	// 读取并打印 CSV 文件内容
	fmt.Println("\n读取文件内容：")
	if err := readFromCSV(filename); err != nil {
		fmt.Println("读取失败:", err)
		return
	}
}

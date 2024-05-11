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
	"equinox"
	"equinox/data_type"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func generateRandomChars(sizeStr string) (string, error) {
	// 定义单位映射
	units := map[string]int{
		"b": 1,                  // bytes
		"k": 1024,               // KB
		"m": 1024 * 1024,        // MB
		"g": 1024 * 1024 * 1024, // GB
	}

	// 转换为小写并去除空格
	sizeStr = strings.ToLower(strings.TrimSpace(sizeStr))

	// 分离数字和单位
	numStr := ""
	unit := ""
	for _, char := range sizeStr {
		if char >= '0' && char <= '9' || char == '.' {
			numStr += string(char)
		} else {
			unit += string(char)
		}
	}

	if numStr == "" || (unit != "" && units[unit] == 0) {
		return "", fmt.Errorf("无效的大小格式，请使用如 '4k', '64b', '2m'")
	}

	// 转换为字节数
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return "", fmt.Errorf("数字解析错误: %v", err)
	}

	multiplier := 1
	if unit != "" {
		multiplier = units[unit]
	}
	sizeBytes := int(num * float64(multiplier))

	// 可用的字符集
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()"
	rand.Seed(time.Now().UnixNano())

	// 生成随机字符串
	var builder strings.Builder
	for i := 0; i < sizeBytes; i++ {
		builder.WriteByte(chars[rand.Intn(len(chars))])
	}

	return builder.String(), nil
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func getTestDB(separate bool) (equinox.DB, func()) {
	dir, _ := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")
	options := equinox.DefaultOptions(dir)
	options.Separate = separate
	db, err := equinox.Open(options)

	if err != nil {
		panic(err)
	}

	return db, func() {
		db.Close()
		removeDir(dir)
	}
}

func testWrite(dataSize int, batchSize int, Separate bool, Sync bool, val string) {
	time.Sleep(5 * time.Second)
	db, fun := getTestDB(Separate)
	time.Sleep(5 * time.Second)
	start := time.Now()
	defer fun()
	point := data_type.New([]byte("string_test"), data_type.STRING)
	writeOptions := &equinox.WriteOptions{Separate: Separate, Sync: Sync}
	for i := 0; i < dataSize; i++ {
		point.Put(uint64(i), val)
		if point.Count() >= batchSize {
			err := db.TSWrite(writeOptions, []data_type.TSEntry{point.DeepCopy()})
			if err != nil {
				fmt.Errorf("error")
			}
			point.Clean()
		}
	}
	duration := time.Since(start)
	ops := float64(dataSize) / duration.Seconds()
	mbps := (float64(dataSize*(4+4+4+len(val)+10)) / (1024 * 1024)) / duration.Seconds()
	fmt.Printf("Total Writes: %d\n", dataSize)
	fmt.Printf("Time Taken: %v\n", duration)
	fmt.Printf("Throughput: %.2f ops/s\n", ops)
	fmt.Printf("Write Speed: %.2f MB/s\n", mbps)
}

func testWriteWithDifferentPointSize() {
	data256, _ := generateRandomChars("256k")
	data64, _ := generateRandomChars("64k")
	data16, _ := generateRandomChars("16k")
	data4, _ := generateRandomChars("4k")
	data1, _ := generateRandomChars("1k")
	data256b, _ := generateRandomChars("256b")
	data64b, _ := generateRandomChars("64b")

	testWrite(40960, 1, true, true, data256)
	testWrite(40960*4, 1, true, true, data64)
	testWrite(40960*16, 1, true, true, data16)
	testWrite(40960*64, 1, true, true, data4)
	testWrite(40960*256, 1, true, true, data1)
	testWrite(40960*1024, 1, true, true, data256b)
	testWrite(40960*1024*4, 1, true, true, data64b)

	testWrite(40960, 1, false, true, data256)
	testWrite(40960*4, 1, false, true, data64)
	testWrite(40960*16, 1, false, true, data16)
	testWrite(40960*64, 1, false, true, data4)
	//
	testWrite(40960*256, 1, false, true, data1)
	testWrite(40960*1024, 1, false, true, data256b)
	testWrite(40960*1024*4, 1, false, true, data64b)
}

func main() {
	//testWriteBoolean(10000000, 100, true, true)
	//time.Sleep(10 * time.Second)
	testWriteWithDifferentPointSize()
}

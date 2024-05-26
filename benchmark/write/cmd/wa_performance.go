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
	"equinox/benchmark/write/utils"
	"equinox/data_type"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

func waWriteTest(dataSize int, batchSize int, Separate bool, Sync bool, val string, writeParallelism int, client int) {
	time.Sleep(5 * time.Second)
	db, fun := getTestDBForParallelism(writeParallelism, Separate)
	time.Sleep(5 * time.Second)

	var wg sync.WaitGroup
	wg.Add(client)
	start := time.Now()
	dataSize = dataSize / client
	for j := 0; j < client; j++ {
		go func(id int) {
			defer wg.Done()
			point := data_type.New([]byte("string_test"+strconv.Itoa(id)), data_type.STRING)
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
			log.Default().Printf("Client %d finished", id)
		}(j)
	}
	wg.Wait()
	var prefix string
	if Separate {
		prefix = "sep"
	} else {
		prefix = "non_sep"
	}
	utils.ExportWASnapshotToCSV(db.GetOption().Metric, prefix+strconv.Itoa(len(val))+"wa_performance")
	fun()
	dataSize = dataSize * client
	duration := time.Since(start)
	ops := float64(dataSize) / duration.Seconds()
	mbps := (float64(dataSize*(4+4+4+len(val)+10)) / (1024 * 1024)) / duration.Seconds()
	fmt.Printf("Total Writes: %d\n", dataSize)
	fmt.Printf("Time Taken: %v s\n", duration)
	fmt.Printf("Throughput: %.2f ops/s\n", ops)
	fmt.Printf("Write Speed: %.2f MB/s\n", mbps)
}

func testWAPerformance() {
	data256, _ := generateRandomChars("256k")
	data64, _ := generateRandomChars("64k")
	data16, _ := generateRandomChars("16k")
	data4, _ := generateRandomChars("4k")
	data1, _ := generateRandomChars("1k")
	data256b, _ := generateRandomChars("256b")
	data64b, _ := generateRandomChars("64b")
	waWriteTest(40960*1024*4, 1, true, true, data64b, 4, 16)
	waWriteTest(40960*1024*4, 1, false, true, data64b, 4, 1)
	return

	waWriteTest(40960*16, 1, true, true, data16, 4, 16)
	waWriteTest(40960*256, 1, true, true, data1, 4, 16)
	waWriteTest(40960*1024*4, 1, true, true, data64b, 4, 16)

	waWriteTest(40960, 1, false, true, data256, 4, 16)
	waWriteTest(40960*16, 1, false, true, data16, 4, 16)
	waWriteTest(40960*256, 1, false, true, data1, 4, 16)
	waWriteTest(40960*1024*4, 1, false, true, data64b, 4, 16)

	return
	for i := 1; i <= 6; i++ {
		parallelismWrite(40960, 1, true, true, data256, i, 4)
		parallelismWrite(40960*4, 1, true, true, data64, i, 4)
		parallelismWrite(40960*16, 1, true, true, data16, i, 4)
		parallelismWrite(40960*64, 1, true, true, data4, i, 4)
		parallelismWrite(40960*256, 1, true, true, data1, i, 4)
		parallelismWrite(40960*1024, 1, true, true, data256b, i, 4)
		parallelismWrite(40960*1024*4, 1, true, true, data64b, i, 4)
		fmt.Printf("finish round: %d\n", i)
	}

	//parallelismWrite(40960, 1, true, true, data256, db)
	//parallelismWrite(40960, 1, true, true, data256, db)
	//parallelismWrite(40960, 1, true, true, data256, db)
	//parallelismWrite(40960, 1, true, true, data256, db)

	//parallelismWrite(40960*4, 1, true, true, data64, writeParallelism)
	//parallelismWrite(40960*16, 1, true, true, data16, writeParallelism)
	//parallelismWrite(40960*64, 1, true, true, data4, writeParallelism)
	//parallelismWrite(40960*256, 1, true, true, data1, writeParallelism)
	//parallelismWrite(40960*1024, 1, true, true, data256b, writeParallelism)
	//parallelismWrite(40960*1024*4, 1, true, true, data64b, writeParallelism)
}

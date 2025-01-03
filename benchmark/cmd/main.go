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
	"equinox/storage"
	"equinox/storage/config"
	"equinox/storage/metric"
	"equinox/storage/types"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

func getTestDB() *storage.Engine {
	dir, _ := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")

	options := config.NewOption()
	options.Dir = dir
	db, _ := storage.NewEngine(options)
	return db
}

func getTestDBNonSep() *storage.Engine {
	dir, _ := ioutil.TempDir("/Users/noorall/GolandProjects/equinox/benchmark/write/db", "equinox-test")

	options := config.NewOption()
	options.SeparateEnabled = false
	options.Dir = dir
	db, _ := storage.NewEngine(options)
	return db
}

func TestWritePoints(e *storage.Engine, wg *sync.WaitGroup) {
	line := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--\n" // 64 bytes
	var builder strings.Builder
	for i := 0; i < 64; i++ {
		builder.WriteString(line)
	}
	data := builder.String()
	fields := types.Fields{
		"field1": data,
		"field4": data,
	}
	start := time.Now()
	for i := 0; i < 2621440/2/10; i++ {
		p, _ := types.NewPoint("hh", fields, time.Now(), types.LifeCycleDefault)
		_ = e.WriteBatch([]types.Point{p})
	}
	fmt.Printf("cost %v s", time.Since(start).Seconds())
	_ = e.Close()
	wg.Done()
}

func TestWritePoints2(e *storage.Engine, wg *sync.WaitGroup) {
	line := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz--\n" // 64 bytes
	var builder strings.Builder
	for i := 0; i < 64; i++ {
		builder.WriteString(line)
	}
	data := builder.String()
	fields := types.Fields{
		"field1": data,
		"field4": data,
	}
	start := time.Now()
	for i := 0; i < 2621440/2/10/2; i++ {
		p, _ := types.NewPoint("hh2", fields, time.Now(), types.LifeCycleDefault)
		_ = e.WriteBatch([]types.Point{p})
	}
	fmt.Printf("cost %v s", time.Since(start).Seconds())
	_ = e.Close()
	wg.Done()
}

func main2() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	log, _ := zap.NewProduction()
	metric.RunMetricServer(log, 2121)
	//wg.Add(1)
	//e := getTestDB()
	//_ = e.Open(context.Background())
	//go TestWritePoints(e, &wg)
	////go TestWritePoints2(e, &wg)
	wg.Wait()
}

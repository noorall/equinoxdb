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

package utils

type TSEntry interface {
	Put(timestamp uint64, value interface{}) error
}

type BoolTSEntry struct {
	timestamp []uint64
	value     []bool
}

func (e *BoolTSEntry) put(timestamp uint64, value bool) {
	e.timestamp = append(e.timestamp, timestamp)
	e.value = append(e.value, value)
}

type IntTSEntry struct {
	timestamp []uint64
	value     []int64
}

func (e *IntTSEntry) put(timestamp uint64, value int64) {
	e.timestamp = append(e.timestamp, timestamp)
	e.value = append(e.value, value)
}

type DoubleTSEntry struct {
	timestamp []uint64
	value     []float64
}

func (e *DoubleTSEntry) put(timestamp uint64, value float64) {
	e.timestamp = append(e.timestamp, timestamp)
	e.value = append(e.value, value)
}

type StringTSEntry struct {
	timestamp []uint64
	value     []string
}

func (e *StringTSEntry) put(timestamp uint64, value string) {
	e.timestamp = append(e.timestamp, timestamp)
	e.value = append(e.value, value)
}

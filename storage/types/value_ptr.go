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

package types

import (
	"fmt"
	"time"
)

type PtrValue struct {
	unixNano int64
	value    []byte
	MinTime  int64
	MaxTime  int64
}

func NewPtrValueValue(min, max int64, v []byte) Value {
	return PtrValue{unixNano: min, MinTime: min, MaxTime: max, value: v}
}

func (v PtrValue) Size() int {
	return len(v.value) + 8
}

func (v PtrValue) UnixNano() int64 {
	return v.unixNano
}

func (v PtrValue) Value() interface{} {
	return v.value
}

func (v PtrValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixNano), v.Value())
}

func (v PtrValue) RawValue() []byte { return v.value }

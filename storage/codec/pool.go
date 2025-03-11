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

package codec

import (
	"equinox/pkg/pool"
	"runtime"
)

func init() {
	// Prime the pools with one encoder/decoder for each available CPU.
	//vals := make([]interface{}, 0, runtime.NumCPU())
	//for _, p := range []*pool.Generic{
	//	timeEncoderPool, timeDecoderPool,
	//	integerEncoderPool, integerDecoderPool,
	//	floatDecoderPool, floatDecoderPool,
	//	stringEncoderPool, stringEncoderPool,
	//	booleanEncoderPool, booleanDecoderPool,
	//} {
	//	vals = vals[:0]
	//	// Check one out to force the allocation now and hold onto it
	//	for i := 0; i < runtime.NumCPU(); i++ {
	//		v := p.Get(config.DefaultMaxPointsPerBlock)
	//		vals = append(vals, v)
	//	}
	//	// Add them all back
	//	for _, v := range vals {
	//		p.Put(v)
	//	}
	//}
}

var (
	// encoder pools

	timeEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewTimeEncoder(sz)
	})
	integerEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewIntegerEncoder(sz)
	})
	floatEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewFloatEncoder()
	})
	stringEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewStringEncoder(sz)
	})
	booleanEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return NewBooleanEncoder(sz)
	})

	// decoder pools

	timeDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &TimeDecoder{}
	})
	integerDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &IntegerDecoder{}
	})
	floatDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &FloatDecoder{}
	})
	stringDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &StringDecoder{}
	})
	booleanDecoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return &BooleanDecoder{}
	})
)

func GetTimeEncoder(sz int) TimeEncoder {
	x := timeEncoderPool.Get(sz).(TimeEncoder)
	x.Reset()
	return x
}
func PutTimeEncoder(enc TimeEncoder) { timeEncoderPool.Put(enc) }

func GetIntegerEncoder(sz int) IntegerEncoder {
	x := integerEncoderPool.Get(sz).(IntegerEncoder)
	x.Reset()
	return x
}
func PutIntegerEncoder(enc IntegerEncoder) { integerEncoderPool.Put(enc) }

func GetUnsignedEncoder(sz int) IntegerEncoder {
	x := integerEncoderPool.Get(sz).(IntegerEncoder)
	x.Reset()
	return x
}
func PutUnsignedEncoder(enc IntegerEncoder) { integerEncoderPool.Put(enc) }

func GetFloatEncoder(sz int) *FloatEncoder {
	x := floatEncoderPool.Get(sz).(*FloatEncoder)
	x.Reset()
	return x
}
func PutFloatEncoder(enc *FloatEncoder) { floatEncoderPool.Put(enc) }

func GetStringEncoder(sz int) StringEncoder {
	x := stringEncoderPool.Get(sz).(StringEncoder)
	x.Reset()
	return x
}
func PutStringEncoder(enc StringEncoder) { stringEncoderPool.Put(enc) }

func GetBooleanEncoder(sz int) BooleanEncoder {
	x := booleanEncoderPool.Get(sz).(BooleanEncoder)
	x.Reset()
	return x
}
func PutBooleanEncoder(enc BooleanEncoder) { booleanEncoderPool.Put(enc) }

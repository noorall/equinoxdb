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
	"sort"
)

type Values []Value

func (a Values) MinTime() int64 {
	return a[0].UnixNano()
}

func (a Values) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a Values) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a Values) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a Values) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a Values) Deduplicate() Values {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a Values) Exclude(min, max int64) Values {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a Values) Include(min, max int64) Values {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a Values) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a Values) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a Values) Merge(b Values) Values {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(Values, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

func (a Values) Encode(buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case FloatValue:
		return encodeFloatBlock(buf, a)
	case IntegerValue:
		return encodeIntegerBlock(buf, a)
	case UnsignedValue:
		return encodeUnsignedBlock(buf, a)
	case BooleanValue:
		return encodeBooleanBlock(buf, a)
	case StringValue:
		return encodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}

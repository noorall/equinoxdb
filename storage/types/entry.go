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
	"equinox/common"
	"sync"
)

type Entry struct {
	sync.RWMutex
	values Values
	vType  byte
}

// NewEntryValues returns a new instance of Entry with the given values.  If the
// values are not valid, an errs is returned.
func NewEntryValues(values []Value) (*Entry, error) {
	e := &Entry{}
	e.values = make(Values, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same types
		if et != valueType(v) {
			return nil, equinox.ErrFieldTypeConflict
		}
	}

	// Set the types of values stored.
	e.vType = et

	return e, nil
}

// Add adds the given values to the Entry.
func (e *Entry) Add(values []Value) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}

	// Are any of the new values the wrong types?
	if e.vType != 0 {
		for _, v := range values {
			if e.vType != valueType(v) {
				return equinox.ErrFieldTypeConflict
			}
		}
	}

	// Entry currently has no values, so add the new ones and we're done.
	e.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.vType = valueType(values[0])
		e.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	e.Unlock()
	return nil
}

// Deduplicate sorts and orders the Entry's values. If values are already deduped and sorted,
// the function does no work and simply returns.
func (e *Entry) Deduplicate() {
	e.Lock()
	defer e.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
}

// Count returns the number of values in this Entry.
func (e *Entry) Count() int {
	e.RLock()
	n := len(e.values)
	e.RUnlock()
	return n
}

// Filter removes all values with timestamps between min and max inclusive.
func (e *Entry) Filter(min, max int64) {
	e.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
	e.Unlock()
}

// Size returns the size of this Entry in bytes.
func (e *Entry) Size() int {
	e.RLock()
	sz := e.values.Size()
	e.RUnlock()
	return sz
}

func (e *Entry) Clean() {
	e.Lock()
	defer e.Unlock()
	e.values = e.values[:0]
}

func (e *Entry) Values() Values {
	return e.values
}

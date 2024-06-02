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

package equinox

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FieldType represents the types of field.
type FieldType int

const (
	// Integer indicates the field's types is integer.
	Integer FieldType = iota

	// Float indicates the field's types is float.
	Float

	// Boolean indicates the field's types is boolean.
	Boolean

	// String indicates the field's types is string.
	String

	// Empty is used to indicate that there is no field.
	Empty

	// Unsigned indicates the field's types is an unsigned integer.
	Unsigned
)

type Point interface {
	Key() []byte

	SetKey(key string)

	Fields() (Fields, error)

	Time() time.Time

	SetTime(t time.Time)

	Iterator() FieldIterator
}

type FieldIterator interface {
	Next() bool

	Type() FieldType

	FieldKey() []byte

	StringValue() string

	IntegerValue() (int64, error)

	BooleanValue() (bool, error)

	FloatValue() (float64, error)

	UnsignedValue() (uint64, error)

	Reset()
}

type point struct {
	time time.Time

	key []byte

	fields []byte

	ts []byte

	cachedFields Fields

	it fieldIterator
}

type fieldIterator struct {
	start, end int
	key        []byte
	valueBuf   []byte
	fieldType  FieldType
}

func (p *point) Key() []byte {
	return p.key
}

func (p *point) SetKey(key string) {
	p.key = []byte(key)
}

func (p *point) Time() time.Time {
	return p.time
}

func (p *point) SetTime(t time.Time) {
	p.time = t
}

func (p *point) Fields() (Fields, error) {
	if p.cachedFields != nil {
		return p.cachedFields, nil
	}
	cf, err := p.unmarshalBinary()
	if err != nil {
		return nil, err
	}
	p.cachedFields = cf
	return p.cachedFields, nil
}

func (p *point) Iterator() FieldIterator {
	return p
}

func (p *point) Next() bool {
	p.it.start = p.it.end
	if p.it.start >= len(p.fields) {
		return false
	}
	p.it.end, p.it.key = scanTo(p.fields, p.it.start, '=')
	p.it.end, p.it.valueBuf = scanFieldValue(p.fields, p.it.end+1)
	p.it.end++
	if len(p.it.valueBuf) == 0 {
		p.it.fieldType = Empty
		return true
	}
	c := p.it.valueBuf[0]
	if c == '"' {
		p.it.fieldType = String
		return true
	}
	if strings.IndexByte(`0123456789-.nNiIu`, c) >= 0 {
		if p.it.valueBuf[len(p.it.valueBuf)-1] == 'i' {
			p.it.fieldType = Integer
			p.it.valueBuf = p.it.valueBuf[:len(p.it.valueBuf)-1]
		} else {
			p.it.fieldType = Float
		}
	} else {
		p.it.fieldType = Boolean
	}
	return true
}

func (p *point) Type() FieldType {
	return p.it.fieldType
}

func (p *point) FieldKey() []byte {
	return p.it.key
}

func (p *point) StringValue() string {
	return string(p.it.valueBuf[1 : len(p.it.valueBuf)-1])
}

func (p *point) IntegerValue() (int64, error) {
	n, err := parseIntBytes(p.it.valueBuf, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse integer value %q: %v", p.it.valueBuf, err)
	}
	return n, nil
}

func (p *point) BooleanValue() (bool, error) {
	b, err := parseBoolBytes(p.it.valueBuf)
	if err != nil {
		return false, fmt.Errorf("unable to parse bool value %q: %v", p.it.valueBuf, err)
	}
	return b, nil
}

func (p *point) FloatValue() (float64, error) {
	f, err := parseFloatBytes(p.it.valueBuf, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse floating point value %q: %v", p.it.valueBuf, err)
	}
	return f, nil
}

func (p *point) UnsignedValue() (uint64, error) {
	n, err := parseUintBytes(p.it.valueBuf, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse unsigned value %q: %v", p.it.valueBuf, err)
	}
	return n, nil
}

func (p *point) Reset() {
	p.it.fieldType = Empty
	p.it.key = nil
	p.it.valueBuf = nil
	p.it.start = 0
	p.it.end = 0
}

func (p *point) unmarshalBinary() (Fields, error) {
	iter := p.Iterator()
	fields := make(Fields, 8)
	for iter.Next() {
		if len(iter.FieldKey()) == 0 {
			continue
		}
		switch iter.Type() {
		case Float:
			v, err := iter.FloatValue()
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			}
			fields[string(iter.FieldKey())] = v
		case Integer:
			v, err := iter.IntegerValue()
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			}
			fields[string(iter.FieldKey())] = v
		case Unsigned:
			v, err := iter.UnsignedValue()
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			}
			fields[string(iter.FieldKey())] = v
		case String:
			fields[string(iter.FieldKey())] = iter.StringValue()
		case Boolean:
			v, err := iter.BooleanValue()
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal field %s: %s", string(iter.FieldKey()), err)
			}
			fields[string(iter.FieldKey())] = v
		default:
			panic("unhandled data types")
		}
	}
	return fields, nil
}

func NewPoint(key string, fields Fields, t time.Time) (Point, error) {
	return &point{
		key:    []byte(key),
		time:   t,
		fields: fields.MarshalBinary(),
	}, nil
}

type Fields map[string]interface{}

func (p Fields) MarshalBinary() []byte {
	sz := len(p) - 1
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
		sz += len(k)
	}
	if len(keys) > 1 {
		sort.Strings(keys)
	}
	b := make([]byte, 0, sz)
	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendField(b, k, p[k])
	}
	return b
}

func appendField(b []byte, k string, v interface{}) []byte {
	b = append(b, []byte(k)...)
	b = append(b, '=')
	switch v := v.(type) {
	case float64:
		b = strconv.AppendFloat(b, v, 'f', -1, 64)
	case int64:
		b = strconv.AppendInt(b, v, 10)
		b = append(b, 'i')
	case string:
		b = append(b, '"')
		b = append(b, []byte(v)...)
		b = append(b, '"')
	case bool:
		b = strconv.AppendBool(b, v)
	case int32:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case int16:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case int8:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case int:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case uint64:
		b = strconv.AppendUint(b, v, 10)
		b = append(b, 'u')
	case uint32:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case uint16:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case uint8:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case uint:
		b = strconv.AppendInt(b, int64(v), 10)
		b = append(b, 'i')
	case float32:
		b = strconv.AppendFloat(b, float64(v), 'f', -1, 32)
	case []byte:
		b = append(b, v...)
	case nil:
	default:
		b = append(b, '"')
		b = append(b, []byte(fmt.Sprintf("%v", v))...)
		b = append(b, '"')

	}
	return b
}

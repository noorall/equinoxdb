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

import "bytes"

func scanTo(buf []byte, i int, stop byte) (int, []byte) {
	start := i
	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}
		// Reached unescaped stop value?
		if buf[i] == stop && (i == 0 || buf[i-1] != '\\') {
			break
		}
		i++
	}
	return i, buf[start:i]
}

func scanFieldValue(buf []byte, i int) (int, []byte) {
	start := i
	quoted := false
	for i < len(buf) {
		// Only escape char for a field value is a double-quote and backslash
		if buf[i] == '\\' && i+1 < len(buf) && (buf[i+1] == '"' || buf[i+1] == '\\') {
			i += 2
			continue
		}
		// Quoted value? (e.g. string)
		if buf[i] == '"' {
			i++
			quoted = !quoted
			continue
		}
		if buf[i] == ',' && !quoted {
			break
		}
		i++
	}
	return i, buf[start:i]
}

func ParseKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	return key[:len(key)-8]
}

func CompareKeys(a, b []byte) int {
	if cmp := bytes.Compare(ParseKey(a), ParseKey(b)); cmp != 0 {
		return cmp
	}

	return bytes.Compare(b[len(a)-8:], a[len(b)-8:])
}

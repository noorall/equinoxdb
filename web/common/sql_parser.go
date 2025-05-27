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

package common

import (
	"equinox/storage/types"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type SQLType int

const (
	Select SQLType = iota
	Insert
	Delete
)

type ParsedSelect struct {
	Key       string
	Field     string
	StartTime int64
	EndTime   int64
	Limit     int
	ASC       bool
}

type ParsedInsert struct {
	Key        string
	Fields     map[string]interface{}
	FieldTypes map[string]types.FieldType
	Time       time.Time
}

type ParsedDelete struct {
	Key       string
	StartTime int64
	EndTime   int64
}

func ParseSQL(sql string) (SQLType, interface{}, error) {
	sql = strings.TrimSpace(sql)
	lower := strings.ToLower(sql)
	if strings.HasPrefix(lower, "select") {
		result, err := parseSelect(sql)
		return Select, result, err
	} else if strings.HasPrefix(lower, "insert") {
		result, err := parseInsert(sql)
		return Insert, result, err
	} else if strings.HasPrefix(lower, "delete") {
		result, err := parseDelete(sql)
		return Delete, result, err
	} else {
		return -1, nil, errors.New("unsupported sql type")
	}
}

func parseSelect(sql string) (*ParsedSelect, error) {
	sql = strings.TrimSuffix(sql, ";")
	asc := strings.HasSuffix(strings.ToLower(strings.TrimSpace(sql)), "asc")
	if strings.HasSuffix(strings.ToLower(sql), " asc") {
		sql = strings.TrimSpace(sql[:len(sql)-4])
	}
	if strings.HasSuffix(strings.ToLower(sql), " desc") {
		sql = strings.TrimSpace(sql[:len(sql)-5])
	}
	r := regexp.MustCompile(`(?i)^select\s+(\w+)\s+from\s+(\w+)(?:\s+where\s+time\s*([><])\s*([^ ;]+)(?:\s+and\s+time\s*([><])\s*([^ ;]+))?)?(?:\s+limit\s+(\d+))?$`)
	matches := r.FindStringSubmatch(sql)
	if matches == nil {
		return nil, errors.New("unable to parse select statements")
	}

	field := matches[1]
	table := matches[2]

	// 初始化默认值
	start := int64(math.MinInt64 + 2)
	end := int64(math.MaxInt64 - 1)
	limit := 10

	// 处理 where 子句
	if matches[3] != "" {
		// 第一个时间条件
		timeValue, err := parseTime(matches[4])
		if err != nil {
			return nil, fmt.Errorf("failed to parse time value: %v", err)
		}
		if matches[3] == ">" {
			start = timeValue + 1
		} else if matches[3] == "<" {
			end = timeValue - 1
		}

		// 第二个时间条件（如果存在）
		if matches[5] != "" {
			timeValue2, err := parseTime(matches[6])
			if err != nil {
				return nil, fmt.Errorf("failed to parse time value:: %v", err)
			}
			if matches[5] == ">" {
				start = timeValue2 + 1
			} else if matches[5] == "<" {
				end = timeValue2 - 1
			}
		}
	}

	// 处理 limit 子句
	if matches[7] != "" {
		parsedLimit, err := strconv.Atoi(matches[7])
		if err != nil {
			return nil, fmt.Errorf("failed to parse limit value:: %v", err)
		}
		limit = parsedLimit
	}

	return &ParsedSelect{
		Key:       table,
		Field:     field,
		StartTime: start,
		EndTime:   end,
		Limit:     limit,
		ASC:       asc,
	}, nil
}

func parseInsert(sql string) (*ParsedInsert, error) {
	sql = strings.TrimSuffix(sql, ";")
	r := regexp.MustCompile(`(?i)^insert into (\w+) (.+);?$`)
	matches := r.FindStringSubmatch(sql)
	if matches == nil {
		return nil, errors.New("failed to parse insert value")
	}
	table := matches[1]
	kvStr := matches[2]
	kvs, kvt, err := splitKeyValuePairs(kvStr)
	if err != nil {
		return nil, err
	}
	values := make(map[string]interface{})
	ts := time.Now()
	for k, v := range kvs {
		if strings.ToLower(k) == "time" {
			ts, err = parseTimeTime(v.(string))
			if err != nil {
				return nil, err
			}
			continue
		}
		values[k] = v
	}
	return &ParsedInsert{Key: table, Fields: values, FieldTypes: kvt, Time: ts}, nil
}

func parseDelete(sql string) (*ParsedDelete, error) {
	sql = strings.TrimSuffix(sql, ";")
	r := regexp.MustCompile(`(?i)^delete\s+from\s+(\w+)(?:\s+where\s+time\s*([><])\s*([^ ;]+)(?:\s+and\s+time\s*([><])\s*([^ ;]+))?)?$`)
	matches := r.FindStringSubmatch(sql)
	if matches == nil {
		return nil, errors.New("failed to parse delete sql")
	}

	table := matches[1]

	// 初始化默认值
	start := int64(math.MinInt64)
	end := int64(math.MaxInt64)

	// 处理 where 子句
	if matches[2] != "" {
		// 第一个时间条件
		timeValue, err := parseTime(matches[3])
		if err != nil {
			return nil, fmt.Errorf("failed to parse time value:: %v", err)
		}
		if matches[2] == ">" {
			start = timeValue + 1
		} else if matches[2] == "<" {
			end = timeValue - 1
		}

		// 第二个时间条件（如果存在）
		if matches[4] != "" {
			timeValue2, err := parseTime(matches[5])
			if err != nil {
				return nil, fmt.Errorf("failed to parse time value:: %v", err)
			}
			if matches[4] == ">" {
				start = timeValue2 + 1
			} else if matches[4] == "<" {
				end = timeValue2 - 1
			}
		}
	}

	return &ParsedDelete{
		Key:       table,
		StartTime: start,
		EndTime:   end,
	}, nil
}

func splitKeyValuePairs(s string) (map[string]interface{}, map[string]types.FieldType, error) {
	fields := make(map[string]interface{})
	fieldTypes := make(map[string]types.FieldType)
	inQuotes := false
	sb := strings.Builder{}
	currentKey := ""
	state := 0 // 0 解析 key，1 解析 value
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '=' && state == 0 {
			currentKey = sb.String()
			sb.Reset()
			state = 1
			continue
		}
		if c == ',' && !inQuotes && state == 1 {
			val, t, err := decodeFieldValue(sb.String(), strings.TrimSpace(currentKey))
			if err != nil {
				return nil, nil, err
			}
			fields[strings.TrimSpace(currentKey)] = val
			fieldTypes[strings.TrimSpace(currentKey)] = t
			sb.Reset()
			state = 0
			continue
		}
		if c == '"' && (i == 0 || s[i-1] != '\\') {
			inQuotes = !inQuotes
		}
		sb.WriteByte(c)
	}
	if state == 1 && currentKey != "" {
		val, t, err := decodeFieldValue(sb.String(), strings.TrimSpace(currentKey))
		if err != nil {
			return nil, nil, err
		}
		fields[strings.TrimSpace(currentKey)] = val
		fieldTypes[strings.TrimSpace(currentKey)] = t
	}
	return fields, fieldTypes, nil
}

func decodeFieldValue(s string, k string) (interface{}, types.FieldType, error) {
	valueStr := strings.TrimSpace(s)
	if strings.ToLower(k) == "time" {
		return valueStr, types.String, nil
	}
	if len(valueStr) == 0 {
		return nil, types.Empty, fmt.Errorf("get an empty field value")
	}
	if len(valueStr) >= 2 && valueStr[0] == '"' && valueStr[len(valueStr)-1] == '"' {
		unquoted, err := strconv.Unquote(valueStr)
		if err != nil {
			return nil, types.Empty, fmt.Errorf("failed to decode %s, err: %v", valueStr, err)
		}
		return unquoted, types.String, nil
	} else if valueStr == "true" || valueStr == "false" {
		val := valueStr == "true"
		return val, types.Boolean, nil
	} else if iVal, err := strconv.Atoi(valueStr); err == nil {
		return iVal, types.Integer, nil
	} else if fVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return fVal, types.Float, nil
	} else if uVal, err := strconv.ParseUint(valueStr[0:len(valueStr)-1], 10, 64); err == nil && valueStr[len(valueStr)-1] == 'u' {
		return uVal, types.Unsigned, nil
	} else {
		return valueStr, types.String, nil
	}
}

func parseTime(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("failed to parse timestamp: " + s)
	}
	if strings.ToLower(s) == "now()" {
		return time.Now().UnixNano(), nil
	}
	// 尝试解析为 int64
	if t, err := strconv.ParseInt(s, 10, 64); err == nil {
		return t, nil
	}
	// 尝试解析为 ISO8601
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.999Z07:00",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano(), nil
		}
	}
	return 0, errors.New("failed to parse timestamp: " + s)
}

func parseTimeTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("failed to parse timestamp: " + s)
	}
	if strings.ToLower(s) == "now()" {
		return time.Now(), nil
	}
	// 尝试解析为 int64
	if t, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, t), nil
	}
	// 尝试解析为 ISO8601
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.999Z07:00",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("failed to parse timestamp: " + s)
}

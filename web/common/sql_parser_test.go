package common

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *ParsedSelect
		wantErr  bool
	}{
		{
			name:  "basic select with default limit",
			input: "select temperature from weather;",
			expected: &ParsedSelect{
				Key:       "weather",
				Field:     "temperature",
				StartTime: math.MinInt64,
				EndTime:   math.MaxInt64,
				Limit:     10,
			},
		},
		{
			name:  "select with limit",
			input: "select humidity from climate limit 5;",
			expected: &ParsedSelect{
				Key:       "climate",
				Field:     "humidity",
				StartTime: math.MinInt64,
				EndTime:   math.MaxInt64,
				Limit:     5,
			},
		},
		{
			name:  "select with time range",
			input: "select value from data where time > 100 and time < 200;",
			expected: &ParsedSelect{
				Key:       "data",
				Field:     "value",
				StartTime: 100,
				EndTime:   200,
				Limit:     10,
			},
		},
		{
			name:  "select with only start time",
			input: "select value from data where time > 100;",
			expected: &ParsedSelect{
				Key:       "data",
				Field:     "value",
				StartTime: 100,
				EndTime:   math.MaxInt64,
				Limit:     10,
			},
		},
		{
			name:  "select with only end time",
			input: "select value from data where time < 500;",
			expected: &ParsedSelect{
				Key:       "data",
				Field:     "value",
				StartTime: math.MinInt64,
				EndTime:   500,
				Limit:     10,
			},
		},
		{
			name:    "invalid select",
			input:   "select from data;",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := parseSelect(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("expected error: %v, got: %v", tt.wantErr, err)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(res, tt.expected) {
				t.Errorf("expected: %+v, got: %+v", tt.expected, res)
			}
		})
	}
}

func unixNanoToTime(unixNano int64) time.Time {
	// 将纳秒分为秒和剩余纳秒
	sec := unixNano / 1e9
	nsec := unixNano % 1e9
	return time.Unix(sec, nsec)
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect ParsedInsert
	}{
		{
			name: "Basic insert with int",
			sql:  "insert into data name=\"abc\", value=42, time=1710000000;",
			expect: ParsedInsert{
				Key: "data",
				Fields: map[string]interface{}{
					"name":  "abc",
					"value": 42,
				},
				Time: unixNanoToTime(1710000000),
			},
		},
		{
			name: "Insert with float and bool",
			sql:  "insert into metrics temp=36.6, active=true, time=1710001000;",
			expect: ParsedInsert{
				Key: "metrics",
				Fields: map[string]interface{}{
					"temp":   36.6,
					"active": true,
				},
				Time: unixNanoToTime(1710001000),
			},
		},
		{
			name: "Insert with escaped string",
			sql:  "insert into logs msg=\"hello\\nworld\", level=\"info\", time=1710002000;",
			expect: ParsedInsert{
				Key: "logs",
				Fields: map[string]interface{}{
					"msg":   "hello\nworld",
					"level": "info",
				},
				Time: unixNanoToTime(1710002000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, res, err := ParseSQL(tt.sql)
			if err != nil {
				t.Fatalf("ParseSQL error: %v", err)
			}
			if typ != Insert {
				t.Fatalf("expected Insert type, got %v", typ)
			}
			parsed := res.(*ParsedInsert)
			if parsed.Key != tt.expect.Key || parsed.Time != tt.expect.Time || !reflect.DeepEqual(parsed.Fields, tt.expect.Fields) {
				t.Errorf("unexpected result:\n got: %+v\nwant: %+v", parsed, tt.expect)
			}
		})
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect ParsedDelete
	}{
		{
			name: "Delete with full time range",
			sql:  "delete from data where time > 1710000000 and time < 1710009999;",
			expect: ParsedDelete{
				Key:       "data",
				StartTime: 1710000000,
				EndTime:   1710009999,
			},
		},
		{
			name: "Delete with only start time",
			sql:  "delete from logs where time > 1710001234;",
			expect: ParsedDelete{
				Key:       "logs",
				StartTime: 1710001234,
				EndTime:   math.MaxInt64,
			},
		},
		{
			name: "Delete with only end time",
			sql:  "delete from metrics where time < 1710005678;",
			expect: ParsedDelete{
				Key:       "metrics",
				StartTime: math.MinInt64,
				EndTime:   1710005678,
			},
		},
		{
			name: "Delete without time",
			sql:  "delete from events;",
			expect: ParsedDelete{
				Key:       "events",
				StartTime: math.MinInt64,
				EndTime:   math.MaxInt64,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, res, err := ParseSQL(tt.sql)
			if err != nil {
				t.Fatalf("ParseSQL error: %v", err)
			}
			if typ != Delete {
				t.Fatalf("expected Delete type, got %v", typ)
			}
			parsed := res.(*ParsedDelete)
			if parsed.Key != tt.expect.Key || parsed.StartTime != tt.expect.StartTime || parsed.EndTime != tt.expect.EndTime {
				t.Errorf("unexpected result:\n got: %+v\nwant: %+v", parsed, tt.expect)
			}
		})
	}
}

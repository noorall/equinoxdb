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

package web

import (
	"context"
	"equinox/storage"
	"equinox/storage/config"
	"equinox/storage/cursor"
	"equinox/storage/types"
	"equinox/web/common"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

var e *storage.Engine

func exec(c *gin.Context) {
	var req struct {
		Sql string `json:"cmd"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.String(200, "Invalid request")
		return
	}
	t, r, err := common.ParseSQL(req.Sql)
	if err != nil {
		c.String(200, fmt.Sprintf("Error while parsing statement: %s", err))
		return
	}
	var result string
	switch t {
	case common.Insert:
		op := r.(*common.ParsedInsert)
		err = updateFieldType(op)
		if err != nil {
			break
		}
		p, _ := types.NewPoint(op.Key, op.Fields, op.Time, types.LifeCycleDefault)
		err = e.Write(p)
		result = "ok"
	case common.Delete:
		op := r.(*common.ParsedDelete)
		err = e.DeleteRange([]byte(op.Key), op.StartTime, op.EndTime)
		result = "ok"
	case common.Select:
		op := r.(*common.ParsedSelect)
		var fieldType types.FieldType
		fieldType, err = getFieldType(op.Key, op.Field)
		if err != nil {
			break
		}
		var cur cursor.Cursor
		cur, err = e.Get([]byte(op.Key), []byte(op.Field), fieldType, op.StartTime, op.EndTime, op.ASC)
		if err != nil {
			break
		}
		result = generateSelectResult(cur, op.Field, op.Limit)
	}
	if err != nil {
		c.String(200, fmt.Sprintf("Error while execute statement: %s", err))
	} else {
		c.String(200, result)
	}
}

func updateFieldType(op *common.ParsedInsert) error {
	for field, fieldType := range op.FieldTypes {
		var entry common.FieldTypeEntry
		result := db.Where(&common.FieldTypeEntry{Key: op.Key, Field: field}).First(&entry)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				err := db.Create(&common.FieldTypeEntry{
					Key:       op.Key,
					Field:     field,
					FieldType: fieldType,
				}).Error
				if err != nil {
					return fmt.Errorf("failed to save field type")
				}
			} else {
				// 其他数据库错误
				return fmt.Errorf("failed to get field type")
			}
		} else {
			// 如果已存在，类型不一致则返回错误
			if entry.FieldType != fieldType {
				return fmt.Errorf("the type of field is inconsistent with existing")
			}
		}
	}
	return nil
}

func getFieldType(key, field string) (types.FieldType, error) {
	var entry common.FieldTypeEntry
	result := db.Where(&common.FieldTypeEntry{Key: key, Field: field}).First(&entry)
	if result.Error != nil {
		if strings.Contains(key, "test") {
			return types.String, nil
		}
		return types.Empty, fmt.Errorf("failed to get field type: %v", result.Error)
	}
	return entry.FieldType, nil
}

func generateSelectResult(c cursor.Cursor, fieldName string, limit int) string {
	defer c.Close()
	var builder strings.Builder
	w := tabwriter.NewWriter(&builder, 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintf(w, "timestamp\t%s\n", fieldName)
	switch cur := c.(type) {
	case cursor.BooleanArrayCursor:
		for a := cur.Next(); a.Len() != 0 && limit > 0; a = cur.Next() {
			for i := 0; i < a.Len() && limit > 0; i++ {
				_, _ = fmt.Fprintf(w, "%s\t%v\n", time.Unix(0, a.Timestamps[i]).Format(time.RFC3339Nano), a.Values[i])
				limit--
			}
		}
	case cursor.StringArrayCursor:
		for a := cur.Next(); a.Len() != 0 && limit > 0; a = cur.Next() {
			for i := 0; i < a.Len() && limit > 0; i++ {
				_, _ = fmt.Fprintf(w, "%s\t%v\n", time.Unix(0, a.Timestamps[i]).Format(time.RFC3339Nano), a.Values[i])
				limit--
			}
		}
	case cursor.FloatArrayCursor:
		for a := cur.Next(); a.Len() != 0 && limit > 0; a = cur.Next() {
			for i := 0; i < a.Len() && limit > 0; i++ {
				_, _ = fmt.Fprintf(w, "%s\t%v\n", time.Unix(0, a.Timestamps[i]).Format(time.RFC3339Nano), a.Values[i])
				limit--
			}
		}
	case cursor.IntegerArrayCursor:
		for a := cur.Next(); a.Len() != 0 && limit > 0; a = cur.Next() {
			for i := 0; i < a.Len() && limit > 0; i++ {
				_, _ = fmt.Fprintf(w, "%s\t%v\n", time.Unix(0, a.Timestamps[i]).Format(time.RFC3339Nano), a.Values[i])
				limit--
			}
		}
	case cursor.UnsignedArrayCursor:
		for a := cur.Next(); a.Len() != 0 && limit > 0; a = cur.Next() {
			for i := 0; i < a.Len() && limit > 0; i++ {
				_, _ = fmt.Fprintf(w, "%s\t%v\n", time.Unix(0, a.Timestamps[i]).Format(time.RFC3339Nano), a.Values[i])
				limit--
			}
		}
	}
	_ = w.Flush()
	return builder.String()
}

func initTestDb() error {
	opt := config.NewOption()
	opt.IsTestDB = true
	opt.SyncWrite = true
	opt.Dir = "/Users/noorall/GolandProjects/equinox/data/equinox-test-persistent"
	_ = os.MkdirAll(opt.Dir, 0777)
	var err error
	e, err = storage.NewEngine(opt)
	if err != nil {
		return err
	}
	return e.Open(context.Background())
}

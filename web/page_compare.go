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

package main

import (
	"encoding/json"
	"equinox/web/common"
	"fmt"
	"github.com/gin-gonic/gin"
	"html/template"
	"net/http"
	"strings"
)

func compareTasks(c *gin.Context) {
	idStr := c.Query("ids")
	var ids []string
	if idStr != "" {
		ids = strings.Split(idStr, ",")
	}
	if len(ids) < 2 {
		c.String(http.StatusBadRequest, "请至少选择两个任务")
		return
	}

	var tasks []common.Task
	if err := db.Where("id IN ?", ids).Find(&tasks).Error; err != nil {
		c.String(http.StatusInternalServerError, "查询失败")
		return
	}

	labels := [][]string{}
	written := []float64{}
	durations := []float64{}
	for _, task := range tasks {
		written = append(written, float64(task.WrittenTotal)/1024.0/1024.0) // 转为MB
		durations = append(durations, task.WriteDuration)
	}

	labels = append(labels, []string{fmt.Sprintf("数据量")})

	c.HTML(http.StatusOK, "compare.html", gin.H{
		"Labels":       toJSON(labels),
		"WrittenData":  toJSON([][]float64{written}),
		"DurationData": toJSON([][]float64{durations}),
	})
}

// 帮助函数转为 JS 可用的 JSON
func toJSON(v interface{}) template.JS {
	b, _ := json.Marshal(v)
	return template.JS(b)
}

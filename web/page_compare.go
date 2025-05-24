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
	"path/filepath"
	"strconv"
	"strings"
)

func compareTasks(c *gin.Context) {
	idStr := c.Query("ids")
	var ids []string
	if idStr != "" {
		ids = strings.Split(idStr, ",")
	}
	if len(ids) < 2 {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, `
			<script>
				alert("请至少选择两个任务");
				history.back();
			</script>
		`)
		return
	}

	var tasks []common.Task
	if err := db.
		Where("id IN ?", ids).
		Order("thread DESC").
		Order("type DESC").
		Find(&tasks).Error; err != nil {
		c.String(http.StatusInternalServerError, "查询失败")
		return
	}

	var charts []common.Chart

	charts = append(charts, generateDataSizeChart(tasks))
	charts = append(charts, generateWrittenDurationChart(tasks))
	charts = append(charts, generateWAChart(tasks))
	charts = append(charts, generateOutPutChart(tasks))

	c.HTML(http.StatusOK, "compare.html", gin.H{
		"Charts":     charts,
		"ChartsJson": toJSON(charts),
	})
}

func generateDataSizeChart(tasks []common.Task) common.Chart {
	chart := common.Chart{Id: "chat-written-total", Title: "写入数据量(MB)", Values: make(map[string][]float64)}
	legendMap := fillLegends(tasks, &chart)
	for _, task := range tasks {
		legend := generateLegend(task)
		label := filepath.Base(task.DataPath)
		if len(chart.Values[label]) < len(legendMap) {
			chart.Values[label] = make([]float64, len(legendMap))
		}
		idx := legendMap[legend]
		chart.Values[label][idx] = float64(task.WrittenSize / 1024 / 1024)
	}
	return chart
}

func generateWrittenDurationChart(tasks []common.Task) common.Chart {
	chart := common.Chart{Id: "chat-duration", Title: "写入耗时(秒)", Values: make(map[string][]float64)}
	legendMap := fillLegends(tasks, &chart)
	for _, task := range tasks {
		legend := generateLegend(task)
		label := filepath.Base(task.DataPath)
		if len(chart.Values[label]) < len(legendMap) {
			chart.Values[label] = make([]float64, len(legendMap))
		}
		idx := legendMap[legend]
		chart.Values[label][idx] = task.WriteDuration
	}
	return chart
}

func generateWAChart(tasks []common.Task) common.Chart {
	chart := common.Chart{Id: "chat-wa", Title: "写入放大(倍)", Values: make(map[string][]float64)}
	legendMap := fillLegends(tasks, &chart)
	for _, task := range tasks {
		legend := generateLegend(task)
		label := filepath.Base(task.DataPath)
		if len(chart.Values[label]) < len(legendMap) {
			chart.Values[label] = make([]float64, len(legendMap))
		}
		idx := legendMap[legend]
		chart.Values[label][idx] = float64(task.WrittenTotal) / float64(task.WrittenSize)
	}
	return chart
}

func generateOutPutChart(tasks []common.Task) common.Chart {
	chart := common.Chart{Id: "chat-output", Title: "写入吞吐(MB/S)", Values: make(map[string][]float64)}
	legendMap := fillLegends(tasks, &chart)
	for _, task := range tasks {
		legend := generateLegend(task)
		label := filepath.Base(task.DataPath)
		if len(chart.Values[label]) < len(legendMap) {
			chart.Values[label] = make([]float64, len(legendMap))
		}
		idx := legendMap[legend]
		chart.Values[label][idx] = float64(task.DataSize) / task.WriteDuration / 1024 / 1024
	}
	return chart
}

func fillLegends(tasks []common.Task, chart *common.Chart) map[string]int {
	legendMap := make(map[string]int)
	idx := 0
	for _, task := range tasks {
		legend := generateLegend(task)
		if _, ok := legendMap[legend]; !ok {
			legendMap[legend] = idx
			chart.Legends = append(chart.Legends, legend)
			idx++
		}
	}
	return legendMap
}

func generateLegend(task common.Task) string {
	builder := strings.Builder{}
	if task.Type == 0 {
		builder.Write([]byte("非键值分离"))
	} else {
		builder.Write([]byte("键值分离"))
	}
	builder.Write([]byte("-"))
	if task.Thread == 1 {
		builder.Write([]byte("单线程"))
	} else {
		builder.Write([]byte(fmt.Sprintf("%d线程", task.Thread)))
	}
	builder.Write([]byte("-任务"))
	builder.Write([]byte(strconv.Itoa(int(task.ID))))
	return builder.String()
}

// 帮助函数转为 JS 可用的 JSON
func toJSON(v interface{}) template.JS {
	b, _ := json.Marshal(v)
	return template.JS(b)
}

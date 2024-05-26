package utils

import (
	"encoding/csv"
	"equinox/metric"
	"fmt"
	"os"
	"path"
	"strconv"
)

func ExportWASnapshotToCSV(metric *metric.Metric, name string) {
	baseDir := "/Users/noorall/GolandProjects/equinox/benchmark/write/result"

	// 创建 CSV 文件
	path := path.Join(baseDir, name+".csv")
	file, err := os.Create(path)
	if err != nil {
		fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// 创建 CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 遍历 sync.Map 并写入数据
	metric.GetWASnapshot().Range(func(key, value interface{}) bool {
		// 确保类型安全
		keyStr, ok1 := key.(uint64)
		valueInt, ok2 := value.(uint64)
		if !ok1 || !ok2 {
			fmt.Printf("Invalid key or value type: key=%v, value=%v\n", key, value)
			return true // 继续遍历
		}

		// 写入一行数据
		if err := writer.Write([]string{strconv.Itoa(int(keyStr)), fmt.Sprintf("%d", valueInt)}); err != nil {
			fmt.Printf("Failed to write row: %v\n", err)
			return false // 停止遍历
		}
		return true // 继续遍历
	})
}

package main

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	outputFile := "/Users/noorall/GolandProjects/equinox/benchmark/data_set/random_data_1.csv"
	stringSize := 1024 * 64
	dataCount := 4000

	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < dataCount; i++ {

		timestamp := time.Now().UnixNano()

		randomBytes := make([]byte, stringSize)
		if _, err := rand.Read(randomBytes); err != nil {
			panic(err)
		}
		randomString := hex.EncodeToString(randomBytes) // 转换为16进制确保可打印字符

		record := []string{
			strconv.FormatInt(timestamp, 10),
			randomString,
		}

		if err := writer.Write(record); err != nil {
			panic(err)
		}

		if i%100 == 0 {
			fmt.Printf("Generated %d records...\n", i)
		}
	}

	fmt.Printf("Successfully generated %d records to %s\n", dataCount, outputFile)
}

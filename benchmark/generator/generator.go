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

const MB = 1024 * 1024
const GB = MB * 1024

func formatSize(size int64) string {
	if size >= GB {
		return fmt.Sprintf("%.2f GB", float64(size)/float64(GB))
	}
	return fmt.Sprintf("%.0f MB", float64(size)/float64(MB))
}

func main() {
	dataSize := 4 * GB
	stringSize := 1024 * 16
	fieldCount := 2

	outputFile := fmt.Sprintf("/Users/noorall/GolandProjects/equinox/benchmark/data_set/random_data_%s.csv", formatSize(int64(dataSize)))
	dataCount := int(float64(dataSize/stringSize/fieldCount) / 2.15)

	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < dataCount; i++ {

		timestamp := time.Now().UnixNano()

		record := []string{
			strconv.FormatInt(timestamp, 10),
		}

		for j := 0; j < fieldCount; j++ {
			randomBytes := make([]byte, stringSize)
			if _, err := rand.Read(randomBytes); err != nil {
				panic(err)
			}
			randomString := hex.EncodeToString(randomBytes)
			record = append(record, randomString)
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

package main

import (
	"context"
	"encoding/csv"
	"equinox/storage/types"
	"fmt"
	"os"
	"strconv"
	"time"
)

func runWriteEnableSeparator() {
	file, err := os.Open("/Users/noorall/GolandProjects/equinox/benchmark/data/random_data.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	e := getTestDB()
	_ = e.Open(context.Background())
	defer e.Close()
	start := time.Now()
	reader := csv.NewReader(file)
	ts := float64(0)
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("读取出错:", err)
			continue
		}
		fields := types.Fields{
			"field1": record[1],
		}
		t, _ := strconv.ParseInt(record[0], 10, 64)
		p, _ := types.NewPoint("hh", fields, time.Unix(0, t), types.LifeCycleDefault)
		e.Write(p)
	}
	ts += time.Since(start).Seconds()
	fmt.Printf("%f MB/s", 10240/ts)
}

func runWriteDisableSeparator() {
	file, err := os.Open("/Users/noorall/GolandProjects/equinox/benchmark/data/random_data.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	e := getTestDBNonSep()
	_ = e.Open(context.Background())
	defer e.Close()

	start := time.Now()
	reader := csv.NewReader(file)
	ts := float64(0)
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("读取出错:", err)
			continue
		}
		fields := types.Fields{
			"field1": record[1],
		}
		t, _ := strconv.ParseInt(record[0], 10, 64)
		p, _ := types.NewPoint("hh", fields, time.Unix(0, t), types.LifeCycleDefault)
		e.Write(p)
	}
	ts += time.Since(start).Seconds()
	fmt.Printf("%f MB/s \n", 10240/ts)
}

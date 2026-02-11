package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
	"tomy_file"
	"tomy_validator/pkg/stats"
)

const (
	FileName = "example_data.tomy"
	NumRows  = 100000
)

func main() {
	fmt.Printf("Generating %d rows of data...\n", NumRows)
	table := generateTable(NumRows)

	fmt.Println("Calculating statistics on generated data:")
	stats.CalculateStats(&table)

	fmt.Printf("Saving to '%s'...\n", FileName)
	if err := table.Serialize(FileName); err != nil {
		log.Fatalf("Serialization failed: %v", err)
	}

	fi, _ := os.Stat(FileName)
	fmt.Printf("File generated successfully. Size: %.2f MB\n", float64(fi.Size())/1024.0/1024.0)
}

func generateTable(rows uint64) tomy_file.ColumnarTable {
	colTimestamp := make([]int64, rows)
	colValue := make([]int64, rows)

	colHost := make([]uint64, 0, rows)
	colHostData := make([]byte, 0, rows*15)
	hosts := []string{"192.168.1.1", "10.0.0.1", "localhost", "db-server", "app-node-01"}

	colLevel := make([]uint64, 0, rows)
	colLevelData := make([]byte, 0, rows*5)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	startTime := time.Now().Unix()
	for i := uint64(0); i < rows; i++ {
		colTimestamp[i] = startTime + int64(i)
		colValue[i] = int64(rand.Intn(10000))

		host := hosts[rand.Intn(len(hosts))]
		colHost = append(colHost, uint64(len(host)))
		colHostData = append(colHostData, []byte(host)...)

		level := levels[rand.Intn(len(levels))]
		colLevel = append(colLevel, uint64(len(level)))
		colLevelData = append(colLevelData, []byte(level)...)
	}
	return tomy_file.ColumnarTable{
		NumRows: rows,
		Columns: []tomy_file.AnyColumn{
			&tomy_file.Int64Column{Name: "timestamp", Values: colTimestamp},
			&tomy_file.Int64Column{Name: "value", Values: colValue},
			&tomy_file.VarcharColumn{Name: "host", Offsets: colHost, Data: colHostData},
			&tomy_file.VarcharColumn{Name: "log_level", Offsets: colLevel, Data: colLevelData},
		},
	}
}

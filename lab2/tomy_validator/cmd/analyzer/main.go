package main

import (
	"fmt"
	"log"
	"tomy_file"
	"tomy_validator/pkg/stats"
)

const (
	FileName = "example_data.tomy"
)

func main() {
	fmt.Printf("Reading from '%s'...\n", FileName)
	table, err := tomy_file.Deserialize(FileName)
	if err != nil {
		log.Fatalf("Deserialization failed: %v", err)
	}
	fmt.Printf("Successfully loaded table with %d rows.\n", table.NumRows)
	stats.CalculateStats(table)
}

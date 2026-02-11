package stats

import (
	"fmt"
	"tomy_file"
)

func CalculateStats(table *tomy_file.ColumnarTable) {
	fmt.Printf("Analyzing statistics for %d rows...\n", table.NumRows)

	for _, col := range table.Columns {
		switch col.GetType() {
		case tomy_file.TypeInt64:
			var sum int64 = 0
			for _, v := range col.(*tomy_file.Int64Column).Values {
				sum += v
			}
			var mean float64
			if len(col.(*tomy_file.Int64Column).Values) > 0 {
				mean = float64(sum) / float64(len(col.(*tomy_file.Int64Column).Values))
			}
			fmt.Printf("[INT64] Column '%s': Mean = %.4f\n", col.GetName(), mean)
		case tomy_file.TypeVarchar:
			asciiCount := 0
			for _, b := range col.(*tomy_file.VarcharColumn).Data {
				if b < 128 {
					asciiCount++
				}
			}
			fmt.Printf("[VARCHAR] Column '%s': ASCII Char Count = %d (Total Bytes: %d)\n", col.GetName(), asciiCount, len(col.(*tomy_file.VarcharColumn).Data))
		}
	}
}

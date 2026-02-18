package expr_test

import (
	"reflect"
	"testing"

	"isbd4/pkg/engine/expr"
	"isbd4/pkg/engine/types"
)

func validate(t *testing.T, result interface{}, expected interface{}) {
	t.Helper()
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestEvaluateComplexExpressions(t *testing.T) {
	rowCount := uint64(3)

	colId := types.NewInt64Column("id", []int64{1, 2, 3})

	colValue := types.NewInt64Column("value", []int64{10, 20, 30})

	colName := types.VarcharChunkColumnFromStrings("name", []string{"alice", "bob", "charlie"})

	batch := &types.ChunkResult{
		RowCount: rowCount,
		Columns: []types.ChunkColumn{
			colId,
			colValue,
			colName,
		},
	}

	mapping := make(map[string]int)
	for i, c := range batch.Columns {
		mapping[c.GetName()] = i
	}

	t.Run("Expr1: (id + 5) * value", func(t *testing.T) {
		idRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeInt64, ColName: "id"}
		valRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeInt64, ColName: "value"}
		lit5 := &expr.LiteralExpr{Value: int64(5), Type: types.ChunkColumnTypeInt64}

		addExpr, err := expr.NewBinaryOp(idRef, lit5, expr.Add)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expr1, err := expr.NewBinaryOp(addExpr, valRef, expr.Multiply)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := expr1.Evaluate(batch, mapping)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		intCol, ok := result.(*types.Int64ChunkColumn)
		if !ok {
			t.Fatalf("expected *types.Int64ChunkColumn, got %T", result)
		}

		validate(t, intCol.Values, []int64{60, 140, 240})
	})

	t.Run("Expr2: STRLEN(name) > 3", func(t *testing.T) {
		nameRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeVarchar, ColName: "name"}
		lit3 := &expr.LiteralExpr{Value: int64(3), Type: types.ChunkColumnTypeInt64}

		strlenExpr, err := expr.NewFunction(expr.StrLen, []expr.Expression{nameRef})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expr2, err := expr.NewBinaryOp(strlenExpr, lit3, expr.GreaterThan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := expr2.Evaluate(batch, mapping)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		boolCol, ok := result.(*types.BooleanChunkColumn)
		if !ok {
			t.Fatalf("expected *types.BooleanChunkColumn, got %T", result)
		}

		validate(t, boolCol.Values, []bool{true, false, true})
	})

	t.Run("Expr3: CONCAT(UPPER(name), '_suffix')", func(t *testing.T) {
		nameRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeVarchar, ColName: "name"}
		suffixLit := &expr.LiteralExpr{Value: "_suffix", Type: types.ChunkColumnTypeVarchar}

		upperExpr, err := expr.NewFunction(expr.Upper, []expr.Expression{nameRef})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expr3, err := expr.NewFunction(expr.Concat, []expr.Expression{upperExpr, suffixLit})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := expr3.Evaluate(batch, mapping)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		varcharCol, ok := result.(*types.VarcharChunkColumn)
		if !ok {
			t.Fatalf("expected *types.VarcharChunkColumn, got %T", result)
		}

		validate(t, varcharCol.GetValuesAsString(), []string{"ALICE_suffix", "BOB_suffix", "CHARLIE_suffix"})
	})

	t.Run("Expr4: REPLACE(name, 'a', 'X')", func(t *testing.T) {
		nameRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeVarchar, ColName: "name"}
		oldLit := &expr.LiteralExpr{Value: "a", Type: types.ChunkColumnTypeVarchar}
		newLit := &expr.LiteralExpr{Value: "X", Type: types.ChunkColumnTypeVarchar}

		replaceExpr, err := expr.NewFunction(expr.Replace, []expr.Expression{nameRef, oldLit, newLit})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := replaceExpr.Evaluate(batch, mapping)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		varcharCol, ok := result.(*types.VarcharChunkColumn)
		if !ok {
			t.Fatalf("expected *types.VarcharChunkColumn, got %T", result)
		}

		validate(t, varcharCol.GetValuesAsString(), []string{"Xlice", "bob", "chXrlie"})
	})

	t.Run("Expr5: SomethingLonger", func(t *testing.T) {
		// (STRLEN(CONCAT(UPPER(name), '!!!')) + id >= 9)
		// AND (true OR false)
		// AND NOT (id = 0)
		// AND (LOWER(name) != 'empty')

		nameRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeVarchar, ColName: "name"}
		idRef := &expr.ColumnRefExpr{ColType: types.ChunkColumnTypeInt64, ColName: "id"}

		upperName, _ := expr.NewFunction(expr.Upper, []expr.Expression{nameRef})

		bangLit := &expr.LiteralExpr{Value: "!!!", Type: types.ChunkColumnTypeVarchar}
		concatExpr, _ := expr.NewFunction(expr.Concat, []expr.Expression{upperName, bangLit})

		strlenExpr, _ := expr.NewFunction(expr.StrLen, []expr.Expression{concatExpr})
		mathAdd, _ := expr.NewBinaryOp(strlenExpr, idRef, expr.Add)

		lit9 := &expr.LiteralExpr{Value: int64(9), Type: types.ChunkColumnTypeInt64}
		comp1, _ := expr.NewBinaryOp(mathAdd, lit9, expr.GreaterEqual)

		resComp1, _ := comp1.Evaluate(batch, mapping)
		validate(t, resComp1.(*types.BooleanChunkColumn).Values, []bool{true, false, true})

		litTrue := &expr.LiteralExpr{Value: true, Type: types.ChunkColumnTypeBoolean}
		litFalse := &expr.LiteralExpr{Value: false, Type: types.ChunkColumnTypeBoolean}
		logicOr, _ := expr.NewBinaryOp(litTrue, litFalse, expr.Or)

		resOr, _ := logicOr.Evaluate(batch, mapping)
		validate(t, resOr.(*types.BooleanChunkColumn).Values, []bool{true, true, true})

		lit0 := &expr.LiteralExpr{Value: int64(0), Type: types.ChunkColumnTypeInt64}
		eqExpr, _ := expr.NewBinaryOp(idRef, lit0, expr.Equal)
		notExpr, _ := expr.NewUnaryOp(eqExpr, expr.Not)

		resNot, _ := notExpr.Evaluate(batch, mapping)
		validate(t, resNot.(*types.BooleanChunkColumn).Values, []bool{true, true, true})

		lowerName, _ := expr.NewFunction(expr.Lower, []expr.Expression{nameRef})
		emptyLit := &expr.LiteralExpr{Value: "empty", Type: types.ChunkColumnTypeVarchar}
		neqExpr, _ := expr.NewBinaryOp(lowerName, emptyLit, expr.NotEqual)

		and1, _ := expr.NewBinaryOp(comp1, logicOr, expr.And)
		resAnd1, _ := and1.Evaluate(batch, mapping)
		validate(t, resAnd1.(*types.BooleanChunkColumn).Values, []bool{true, false, true})

		and2, _ := expr.NewBinaryOp(and1, notExpr, expr.And)
		resAnd2, _ := and2.Evaluate(batch, mapping)
		validate(t, resAnd2.(*types.BooleanChunkColumn).Values, []bool{true, false, true})

		finalExpr, _ := expr.NewBinaryOp(and2, neqExpr, expr.And)

		result, err := finalExpr.Evaluate(batch, mapping)
		if err != nil {
			t.Fatalf("unexpected error during evaluation: %v", err)
		}

		boolCol, ok := result.(*types.BooleanChunkColumn)
		if !ok {
			t.Fatalf("expected *types.BooleanChunkColumn, got %T", result)
		}

		validate(t, boolCol.Values, []bool{true, false, true})
	})
}

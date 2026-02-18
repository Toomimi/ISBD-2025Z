package openapi

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type QueryDefinition interface {
	IsQuery() bool
}

func (s SelectQuery) IsQuery() bool { return true }
func (c CopyQuery) IsQuery() bool   { return true }

type QueryQueryDefinition struct {
	Definition QueryDefinition
}

func (q *QueryQueryDefinition) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	_, hasSource := raw["sourceFilepath"]
	_, hasDest := raw["destinationTableName"]
	_, hasColumns := raw["columnClauses"]

	isCopy := hasSource || hasDest
	isSelect := hasColumns

	if isCopy && isSelect {
		return fmt.Errorf("ambiguous query definition: contains both COPY (sourceFilepath/destinationTableName) and SELECT (columnClauses) fields")
	}

	if isCopy {
		var copyQ CopyQuery
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&copyQ); err != nil {
			return fmt.Errorf("invalid COPY query: %w", err)
		}
		q.Definition = copyQ
		return nil
	}

	if isSelect {
		var selectQ SelectQuery
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&selectQ); err != nil {
			return fmt.Errorf("invalid SELECT query: %w", err)
		}
		q.Definition = selectQ
		return nil
	}

	return fmt.Errorf("incompatible query definition: match neither COPY (sourceFilepath) nor SELECT (columnClauses)")
}

func (q QueryQueryDefinition) MarshalJSON() ([]byte, error) {
	return json.Marshal(q.Definition)
}

// AssertQueryQueryDefinitionRequired checks if the required fields are not zero-ed
func AssertQueryQueryDefinitionRequired(obj QueryQueryDefinition) error {
	if obj.Definition == nil {
		return fmt.Errorf("query definition is empty")
	}

	switch q := obj.Definition.(type) {
	case SelectQuery:
		return AssertSelectQueryRequired(q)
	case CopyQuery:
		return AssertCopyQueryRequired(q)
	default:
		return fmt.Errorf("unknown query definition type")
	}
}

// AssertQueryQueryDefinitionConstraints checks if the values respects the defined constraints
func AssertQueryQueryDefinitionConstraints(obj QueryQueryDefinition) error {
	if obj.Definition == nil {
		return nil
	}

	switch q := obj.Definition.(type) {
	case SelectQuery:
		return AssertSelectQueryConstraints(q)
	case CopyQuery:
		return AssertCopyQueryConstraints(q)
	}

	return nil
}

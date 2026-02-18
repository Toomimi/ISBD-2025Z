package types

import (
	"fmt"

	"isbd4/openapi"
)

type ValidationError struct {
	Problems []ErrWithCtx
}

type ErrWithCtx struct {
	Error   string
	Context string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed with %d problems", len(e.Problems))
}

func (e *ValidationError) Add(err string, context string) {
	e.Problems = append(e.Problems, ErrWithCtx{
		Error:   err,
		Context: context,
	})
}

func (e *ValidationError) Extend(other error) {
	switch otherErr := other.(type) {
	case *ValidationError:
		e.Problems = append(e.Problems, otherErr.Problems...)
	default:
		e.Add(other.Error(), "")
	}
}

func (e *ValidationError) HasProblems() bool {
	return len(e.Problems) > 0
}

func (e *ValidationError) ToOpenAPI() openapi.MultipleProblemsError {
	problems := make([]openapi.MultipleProblemsErrorProblemsInner, len(e.Problems))
	for i, p := range e.Problems {
		problems[i] = openapi.MultipleProblemsErrorProblemsInner{
			Error:   p.Error,
			Context: p.Context,
		}
	}
	return openapi.MultipleProblemsError{
		Problems: problems,
	}
}

func NewVErr(err string, context string) error {
	return &ValidationError{
		Problems: []ErrWithCtx{
			{
				Error:   err,
				Context: context,
			},
		},
	}
}

func ToOpenApiError(err error) openapi.MultipleProblemsError {
	switch e := err.(type) {
	case *ValidationError:
		return e.ToOpenAPI()
	default:
		return openapi.MultipleProblemsError{
			Problems: []openapi.MultipleProblemsErrorProblemsInner{
				{
					Error:   err.Error(),
					Context: "",
				},
			},
		}
	}
}

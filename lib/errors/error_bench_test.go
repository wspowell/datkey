package errors_test

import (
	goerrors "errors" //nolint:depguard // reason: This package is intended to wrap "errors" and therefore needs to import it.
	"fmt"
	"testing"

	"datkey/lib/errors"
)

type BenchError errors.Cause

const (
	BenchFailure = BenchError(iota + 1)
)

const errorMessage = "error message"

//nolint:gochecknoglobals // Global storage to ensure benchmarks do no optimize out benched operations.
var (
	errGlobal       error
	errStringGlobal string
)

func BenchmarkErrorsNewGolang(b *testing.B) {
	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = goerrors.New(errorMessage) //nolint:goerr113 // reason: Ignore for benchmarking.
	}

	b.StopTimer()

	errGlobal = err
}

func BenchmarkErrorsNew(b *testing.B) {
	var err *errors.Error[BenchError]

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = errors.New(BenchFailure, errorMessage)
	}

	b.StopTimer()

	errGlobal = err
}

func BenchmarkErrorsWrapGolang(b *testing.B) {
	err := goerrors.New(errorMessage) //nolint:goerr113 // reason: Ignore for benchmarking.

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = fmt.Errorf("%w", err)
	}

	b.StopTimer()

	errGlobal = err
}

func BenchmarkErrorsNewFromError(b *testing.B) {
	var err *errors.Error[BenchError]
	originalErr := errors.New(BenchFailure, errorMessage)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = errors.NewFromError(BenchFailure, originalErr)
	}

	b.StopTimer()

	errGlobal = err
}

func BenchmarkErrorsErrorGolang(b *testing.B) {
	err := goerrors.New(errorMessage) //nolint:goerr113 // reason: Ignore for benchmarking.
	var errString string

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		errString = err.Error()
	}

	b.StopTimer()

	errStringGlobal = errString
}

func BenchmarkErrorsError(b *testing.B) {
	err := errors.New(BenchFailure, errorMessage)
	var errString string

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		errString = err.Error()
	}

	b.StopTimer()

	errStringGlobal = errString
}

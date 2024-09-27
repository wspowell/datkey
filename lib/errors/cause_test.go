package errors_test

import (
	"testing"

	"datkey/lib/errors"
)

type TestError errors.Cause

const (
	Internal = TestError(iota + 1)
	Canceled
)

func TestCause_enum(t *testing.T) {
	t.Parallel()

	if err := errors.New(Canceled, "canceled"); err != nil {
		switch err.Cause {
		case Internal:
			t.Error("unexpected 'Internal' case invoked")
		case Canceled:
			// Should fall into this case.
		default:
			t.Error("unexpected 'default' case invoked")
		}
	}
}

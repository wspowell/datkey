package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrappedErr_Format(t *testing.T) {
	t.Parallel()

	err := wrappedError{
		err:        nil,
		format:     "formatErr",
		formatArgs: nil,
	}

	assert.Equal(t, "formatErr", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

func TestWrappedErr_Format_Args(t *testing.T) {
	t.Parallel()

	err := wrappedError{
		err:        nil,
		format:     "formatErr: %s",
		formatArgs: []any{"test"},
	}

	assert.Equal(t, "formatErr: test", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

func TestWrappedErr_Error(t *testing.T) {
	t.Parallel()

	err := wrappedError{
		err:        New(CauseInternal, "internal"),
		format:     "",
		formatArgs: nil,
	}

	assert.Equal(t, "internal", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

func TestWrappedErr_Error_ignores_format(t *testing.T) {
	t.Parallel()

	err := wrappedError{
		err:        New(CauseInternal, "internal"),
		format:     "formatErr: %s",
		formatArgs: []any{"test"},
	}

	assert.Equal(t, "internal", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

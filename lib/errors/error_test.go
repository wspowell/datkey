package errors_test

import (
	goerrors "errors" //nolint:depguard // reason: Requires golang errors for testing.
	"testing"

	"github.com/stretchr/testify/assert"

	"datkey/lib/errors"
)

func TestErrors_New(t *testing.T) {
	t.Parallel()

	err := errors.New(errors.CauseInternal, "internal")
	assert.Equal(t, errors.CauseInternal, err.Cause)
	assert.Equal(t, "internal", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

func TestErrors_NewFromError_Error(t *testing.T) {
	t.Parallel()

	originalErr := errors.New(errors.CauseInternal, "internal")
	err := errors.NewFromError(errors.CauseInternal, originalErr)
	assert.Equal(t, errors.CauseInternal, err.Cause)
	assert.Equal(t, "internal", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

func TestErrors_NewFromError_GoError(t *testing.T) {
	t.Parallel()

	originalErr := goerrors.New("internal") //nolint:err113 // reason: Create golang error for testing.
	err := errors.NewFromError(errors.CauseInternal, originalErr)
	assert.Equal(t, errors.CauseInternal, err.Cause)
	assert.Equal(t, "internal", err.Error())
	assert.Equal(t, err.Error(), err.String())
}

package errors

import (
	"errors" //nolint:depguard // reason: This shadows golang "errors" for feature parity.
)

//nolint:gochecknoglobals // reason: This shadows golang "errors" for feature parity.
var (
	As = errors.As
	Is = errors.Is
)

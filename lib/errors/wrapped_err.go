package errors

import "fmt"

// wrappedError allows deferment of formatting any error string.
// This improves performance for errors that are created but never actually printed.
type wrappedError struct {
	err        error
	format     string
	formatArgs []any
}

func (self wrappedError) Error() string {
	return self.String()
}

func (self wrappedError) String() string {
	// Use the stored error, if exists. Otherwise, use the format string.
	if self.err != nil {
		return self.err.Error()
	}

	// Do not invoke fmt.Sprintf() if not necessary to avoid performance impact.
	if len(self.formatArgs) != 0 {
		return fmt.Sprintf(self.format, self.formatArgs...)
	}
	return self.format
}

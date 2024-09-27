package errors

type Error[T Causer] struct {
	// Cause is a machine readable value to provide explicit error detection and exhaustiveness, if desired.
	Cause T

	// err is the human readable message of what error occurred. This will be printed on Error().
	err wrappedError
}

func New[T Causer](cause T, format string, formatArgs ...any) *Error[T] {
	newErr := &Error[T]{
		Cause: cause,
		err: wrappedError{
			err:        nil,
			format:     format,
			formatArgs: formatArgs,
		},
	}

	return newErr
}

func NewFromError[T Causer](cause T, err error) *Error[T] {
	newErr := &Error[T]{
		Cause: cause,
		err: wrappedError{
			err:        err,
			format:     "",
			formatArgs: nil,
		},
	}

	return newErr
}

func (self *Error[T]) Error() string {
	return self.err.String()
}

func (self *Error[T]) String() string {
	return self.err.String()
}

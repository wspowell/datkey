package errors

const (
	CauseInternal = uint(iota + 1)
	CauseCanceled
	CauseNotFound
)

type Cause uint

// Causer is a machine readable error reason. This can be used in a switch statement
// to detect which error occurred and to provide exhaustive error handling.
//
// A cause will never be printed. Any error message should be provided by the error
// message input.
type Causer interface {
	~uint
}

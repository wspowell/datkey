package datkey

import (
	"fmt"
	"sync"
	"time"

	"github.com/wspowell/datkey/lib/errors"
)

//nolint:gochecknoglobals // reason: sync.Pool is one of the few exceptions that makes sense as a global.
var (
	poolValueResponse = sync.Pool{
		New: func() any {
			return newResponse[valueResponse]()
		},
	}

	poolStatsResponse = sync.Pool{
		New: func() any {
			return newResponse[statsResponse]()
		},
	}

	poolTtlResponse = sync.Pool{
		New: func() any {
			return newResponse[ttlResponse]()
		},
	}
)

func poolGetValueResponse() *response[valueResponse] {
	resp := poolValueResponse.Get()

	valueResp, ok := resp.(*response[valueResponse])
	if !ok {
		panic(fmt.Sprintf("invalid type found in poolValueResponse: %T", resp))
	}

	valueResp.reset()
	return valueResp
}

func poolPutValueResponse(valueResp *response[valueResponse]) {
	if valueResp != nil {
		poolValueResponse.Put(valueResp)
	}
}

func poolGetStatsResponse() *response[statsResponse] {
	resp := poolStatsResponse.Get()

	statsResp, ok := resp.(*response[statsResponse])
	if !ok {
		panic(fmt.Sprintf("invalid type found in poolStatsResponse: %T", resp))
	}

	statsResp.reset()
	return statsResp
}

func poolPutStatsResponse(statsResp *response[statsResponse]) {
	if statsResp != nil {
		poolStatsResponse.Put(statsResp)
	}
}

func poolGetTtlResponse() *response[ttlResponse] {
	resp := poolTtlResponse.Get()

	ttlResp, ok := resp.(*response[ttlResponse])
	if !ok {
		panic(fmt.Sprintf("invalid type found in poolTtlResponse: %T", resp))
	}

	ttlResp.reset()
	return ttlResp
}

func poolPutTtlResponse(ttlResp *response[ttlResponse]) {
	if ttlResp != nil {
		poolTtlResponse.Put(ttlResp)
	}
}

type response[T any] struct {
	deadline *time.Ticker
	result   chan T
}

func newResponse[T any]() *response[T] {
	deadline := time.NewTicker(time.Nanosecond)
	return &response[T]{
		deadline: deadline,
		// Size of 1 allows a result to be sent even if the response timed out.
		// Otherwise, there will be no channel reader and it will deadlock.
		result: make(chan T, 1),
	}
}

func (self *response[T]) reset() {
	// Drain the result channel.
	for {
		var done bool
		select {
		case <-self.result:
			// Throw away the value.
		default:
			// Done draining.
			done = true
		}
		if done {
			break
		}
	}
}

func (self *response[T]) send(result T) {
	self.result <- result
}

type responseError errors.Cause

const (
	canceled = responseError(iota)
)

func (self *response[T]) await(timeout time.Duration) (T, *errors.Error[responseError]) {
	// Note: context.Context can do cancellation signals, but they are heavier and consume (relatively) a lot of time.
	// We can replace this with time.Ticker which provides the same functionality we need here, but much faster.
	// Performance is even higher with sync.Pool since we can reset the existing structure without having to create new ones.
	self.deadline.Reset(timeout)
	select {
	case <-self.deadline.C:
		self.deadline.Stop()
		var zero T
		return zero, errors.New(canceled, "response did not complete before deadline")
	case result := <-self.result:
		self.deadline.Stop()
		return result, nil
	}
}

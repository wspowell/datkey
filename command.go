package datkey

import (
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/wspowell/datkey/hash"
	"github.com/wspowell/datkey/lib/errors"
)

type empty = struct{}

type command any

type commandPing struct {
	Resp *response[empty]
}

type commandStats struct {
	Resp *response[statsResponse]
}

type statsResponse struct {
	sizeInBytes int64
}

type StatsResponse struct {
	DbSizeInBytes int64
}

type commandSet struct {
	Resp *response[valueResponse]
	Key  string
	data keyStorage
}

type SetResponse struct {
	PreviousValue []byte
	Exists        bool
}

type commandGet struct {
	Resp *response[valueResponse]
	Key  string
}

type GetResponse struct {
	Value  []byte
	Exists bool
}

type commandDelete struct {
	Resp *response[valueResponse]
	Key  string
}

type DeleteResponse struct {
	DeletedValue []byte
	Exists       bool
}

type commandDeleteExpired struct {
	Resp *response[valueResponse]
}

type commandExpire struct {
	Resp      *response[valueResponse]
	ExpiresAt time.Time
	Key       string
}

type ExpireResponse struct {
	Exists bool
}

type commandPersist struct {
	Resp *response[valueResponse]
	Key  string
}

type PersistResponse struct {
	Exists bool
}

type commandTtl struct {
	Resp *response[ttlResponse]
	Key  string
}

type TtlResponse struct {
	Ttl    time.Duration
	Exists bool
}

type valueResponse struct {
	Value  []byte
	Exists bool
}

type ttlResponse struct {
	Ttl    time.Duration
	Exists bool
}

type keyStorage struct {
	lastAccessTime time.Time
	expiresAt      time.Time
	value          []byte
}

type commandDeleteLru struct {
	Resp *response[valueResponse]
}

func (self keyStorage) isExpired() bool {
	return !self.expiresAt.IsZero() && self.expiresAt.Before(time.Now())
}

func setKey(key string, value []byte, ttl time.Duration, slotCommandInput []chan<- command, commandTimeout time.Duration) (SetResponse, *errors.Error[DbWriteErr]) {
	var expiresAt time.Time
	if ttl != 0 {
		expiresAt = time.Now().Add(ttl)
	}

	data := keyStorage{
		lastAccessTime: time.Now(),
		value:          value,
		expiresAt:      expiresAt,
	}

	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, key string, data keyStorage, resp *response[valueResponse]) {
		slotCommands <- commandSet{
			Key:  key,
			data: data,
			Resp: resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, data, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero SetResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbWriteCanceled, err)
		default:
			return zero, errors.NewFromError(DbWriteInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return SetResponse{
		PreviousValue: result.Value,
		Exists:        result.Exists,
	}, nil
}

func getKey(key string, slotCommandInput []chan<- command, commandTimeout time.Duration) (GetResponse, *errors.Error[DbReadErr]) {
	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, key string, resp *response[valueResponse]) {
		slotCommands <- commandGet{
			Key:  key,
			Resp: resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero GetResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbReadCanceled, err)
		default:
			return zero, errors.NewFromError(DbReadInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return GetResponse(result), nil
}

func deleteKey(key string, slotCommandInput []chan<- command, commandTimeout time.Duration) (DeleteResponse, *errors.Error[DbWriteErr]) {
	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, key string, resp *response[valueResponse]) {
		slotCommands <- commandDelete{
			Key:  key,
			Resp: resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero DeleteResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbWriteCanceled, err)
		default:
			return zero, errors.NewFromError(DbWriteInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return DeleteResponse{
		DeletedValue: result.Value,
		Exists:       result.Exists,
	}, nil
}

func expireKey(key string, ttl time.Duration, slotCommandInput []chan<- command, commandTimeout time.Duration) (ExpireResponse, *errors.Error[DbWriteErr]) {
	expiresAt := time.Now().Add(ttl)

	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, key string, expiresAt time.Time, resp *response[valueResponse]) {
		slotCommands <- commandExpire{
			Key:       key,
			ExpiresAt: expiresAt,
			Resp:      resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, expiresAt, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero ExpireResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbWriteCanceled, err)
		default:
			return zero, errors.NewFromError(DbWriteInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return ExpireResponse{
		Exists: result.Exists,
	}, nil
}

func persistKey(key string, slotCommandInput []chan<- command, commandTimeout time.Duration) (PersistResponse, *errors.Error[DbWriteErr]) {
	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, key string, resp *response[valueResponse]) {
		slotCommands <- commandPersist{
			Key:  key,
			Resp: resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero PersistResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbWriteCanceled, err)
		default:
			return zero, errors.NewFromError(DbWriteInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return PersistResponse{
		Exists: result.Exists,
	}, nil
}

func ttlKey(key string, slotCommandInput []chan<- command, commandTimeout time.Duration) (TtlResponse, *errors.Error[DbReadErr]) {
	resp := poolGetTtlResponse()

	go func(slotCommands chan<- command, key string, resp *response[ttlResponse]) {
		slotCommands <- commandTtl{
			Key:  key,
			Resp: resp,
		}
	}(slotCommandInput[hash.ToSlot(key)], key, resp)

	result, err := resp.await(commandTimeout)
	if err != nil {
		var zero TtlResponse
		switch err.Cause {
		case canceled:
			return zero, errors.NewFromError(DbReadCanceled, err)
		default:
			return zero, errors.NewFromError(DbReadInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutTtlResponse(resp)

	return TtlResponse(result), nil
}

func getDbStats(slotCommandInput []chan<- command, commandTimeout time.Duration) (StatsResponse, *errors.Error[DbReadErr]) {
	mutex := &sync.Mutex{}
	dbStats := StatsResponse{
		DbSizeInBytes: 0,
	}

	group := errgroup.Group{}

	for hashSlot := range hash.MaxHashSlot {
		group.Go(func() error {
			resp := poolGetStatsResponse()

			go func(slotCommands chan<- command, resp *response[statsResponse]) {
				slotCommands <- commandStats{
					Resp: resp,
				}
			}(slotCommandInput[hashSlot], resp)

			result, err := resp.await(commandTimeout)
			if err != nil {
				switch err.Cause {
				case canceled:
					return errors.NewFromError(DbReadCanceled, err)
				default:
					return errors.NewFromError(DbReadInternal, err)
				}
			}

			// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
			poolPutStatsResponse(resp)

			mutex.Lock()
			dbStats.DbSizeInBytes += result.sizeInBytes
			mutex.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return dbStats, err.(*errors.Error[DbReadErr]) //nolint:errorlint,forcetypeassert,revive // reason: It is not possible to be any other type.
	}

	return dbStats, nil
}

func deleteExpired(hashSlot hash.Slot, slotCommandInput []chan<- command, commandTimeout time.Duration) *errors.Error[DbWriteErr] {
	resp := poolGetValueResponse()

	go func(slotCommands chan<- command, resp *response[valueResponse]) {
		slotCommands <- commandDeleteExpired{
			Resp: resp,
		}
	}(slotCommandInput[hashSlot], resp)

	_, err := resp.await(commandTimeout)
	if err != nil {
		switch err.Cause {
		case canceled:
			return errors.NewFromError(DbWriteCanceled, err)
		default:
			return errors.NewFromError(DbWriteInternal, err)
		}
	}

	// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
	poolPutValueResponse(resp)

	return nil
}

func deleteLru(slotCommandInput []chan<- command, commandTimeout time.Duration) *errors.Error[DbWriteErr] {
	group := errgroup.Group{}

	for hashSlot := range hash.MaxHashSlot {
		group.Go(func() error {
			resp := poolGetValueResponse()

			go func(slotCommands chan<- command, resp *response[valueResponse]) {
				slotCommands <- commandDeleteLru{
					Resp: resp,
				}
			}(slotCommandInput[hashSlot], resp)

			_, err := resp.await(commandTimeout)
			if err != nil {
				switch err.Cause {
				case canceled:
					return errors.NewFromError(DbWriteCanceled, err)
				default:
					return errors.NewFromError(DbWriteInternal, err)
				}
			}

			// If success, put response back. Otherwise, we have no idea when the request might complete and may still be used.
			poolPutValueResponse(resp)

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err.(*errors.Error[DbWriteErr]) //nolint:errorlint,forcetypeassert,revive // reason: It is not possible to be any other type.
	}

	return nil
}

package datkey

import (
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/wspowell/datkey/hash"
)

type empty = struct{}

type command any

type commandPing struct {
	Resp empty
}

type commandStats struct {
	Resp *statsResponse
}

type statsResponse struct {
	sizeInBytes int64
}

type StatsResponse struct {
	DbSizeInBytes int64
}

type commandSet struct {
	Resp *valueResponse
	Key  string
	data keyStorage
}

type SetResponse struct {
	PreviousValue []byte
	Exists        bool
}

type commandGet struct {
	Resp *valueResponse
	Key  string
}

type GetResponse struct {
	Value  []byte
	Exists bool
}

type commandDelete struct {
	Resp *valueResponse
	Key  string
}

type DeleteResponse struct {
	DeletedValue []byte
	Exists       bool
}

type commandDeleteExpired struct {
	Resp *valueResponse
}

type commandExpire struct {
	Resp      *valueResponse
	ExpiresAt time.Time
	Key       string
}

type ExpireResponse struct {
	Exists bool
}

type commandPersist struct {
	Resp *valueResponse
	Key  string
}

type PersistResponse struct {
	Exists bool
}

type commandTtl struct {
	Resp *ttlResponse
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
	Resp *valueResponse
}

func (self keyStorage) isExpired() bool {
	return !self.expiresAt.IsZero() && self.expiresAt.Before(time.Now())
}

func setKey(key string, value []byte, ttl time.Duration, cache cacheStorage) SetResponse {
	var expiresAt time.Time
	if ttl != 0 {
		expiresAt = time.Now().Add(ttl)
	}

	data := keyStorage{
		lastAccessTime: time.Now(),
		value:          value,
		expiresAt:      expiresAt,
	}

	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandSet{
		Key:  key,
		data: data,
		Resp: resp,
	})

	return SetResponse{
		PreviousValue: resp.Value,
		Exists:        resp.Exists,
	}
}

func getKey(key string, cache cacheStorage) GetResponse {
	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandGet{
		Key:  key,
		Resp: resp,
	})

	return GetResponse{
		Value:  resp.Value,
		Exists: resp.Exists,
	}
}

func deleteKey(key string, cache cacheStorage) DeleteResponse {
	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandDelete{
		Key:  key,
		Resp: resp,
	})

	return DeleteResponse{
		DeletedValue: resp.Value,
		Exists:       resp.Exists,
	}
}

func expireKey(key string, ttl time.Duration, cache cacheStorage) ExpireResponse {
	expiresAt := time.Now().Add(ttl)

	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandExpire{
		Key:       key,
		ExpiresAt: expiresAt,
		Resp:      resp,
	})

	return ExpireResponse{
		Exists: resp.Exists,
	}
}

func persistKey(key string, cache cacheStorage) PersistResponse {
	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandPersist{
		Key:  key,
		Resp: resp,
	})

	return PersistResponse{
		Exists: resp.Exists,
	}
}

func ttlKey(key string, cache cacheStorage) TtlResponse {
	resp := &ttlResponse{
		Ttl:    0,
		Exists: false,
	}

	cache.runCommand(hash.ToSlot(key), commandTtl{
		Key:  key,
		Resp: resp,
	})

	return TtlResponse{
		Ttl:    resp.Ttl,
		Exists: resp.Exists,
	}
}

func getDbStats(cache cacheStorage) StatsResponse {
	mutex := &sync.Mutex{}
	dbStats := StatsResponse{
		DbSizeInBytes: 0,
	}

	group := errgroup.Group{}

	for hashSlot := range hash.MaxHashSlot {
		group.Go(func() error {
			resp := &statsResponse{
				sizeInBytes: 0,
			}

			cache.runCommand(hashSlot, commandStats{
				Resp: resp,
			})

			mutex.Lock()
			dbStats.DbSizeInBytes += resp.sizeInBytes
			mutex.Unlock()

			return nil
		})
	}

	_ = group.Wait() // The goroutines return no error.

	return dbStats
}

func deleteExpired(hashSlot hash.Slot, cache cacheStorage) {
	resp := &valueResponse{
		Value:  nil,
		Exists: false,
	}

	cache.runCommand(hashSlot, commandDeleteExpired{
		Resp: resp,
	})
}

func deleteLru(cache cacheStorage) {
	group := errgroup.Group{}

	for hashSlot := range hash.MaxHashSlot {
		group.Go(func() error {
			resp := &valueResponse{
				Value:  nil,
				Exists: false,
			}

			cache.runCommand(hashSlot, commandDeleteLru{
				Resp: resp,
			})

			return nil
		})
	}

	_ = group.Wait() // The goroutines return no error
}

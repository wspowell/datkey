package datkey

import (
	"context"
	"time"

	"github.com/wspowell/datkey/lib/errors"
)

type EvictionStrategy string

const (
	EvictDisabled = EvictionStrategy("evictionDisabled")
	EvictByLRU    = EvictionStrategy("leastRecentlyUsed")
	EvictByTTL    = EvictionStrategy("timeToLive")
)

type DbWriteErr errors.Cause

const (
	DbWriteInternal = DbWriteErr(iota)
	DbWriteCanceled
)

type DbReadErr errors.Cause

const (
	DbReadInternal = DbReadErr(iota)
	DbReadCanceled
)

type Config struct {
	// EvictStrategy for when the database reaches a threshold and must begin deleting keys to make room.
	// Default: EvictByLRU
	EvictStrategy EvictionStrategy

	// DbBytesEvictThreshold, in bytes, when keys will start being evicted to make room for other keys.
	// Default: None (0)
	DbBytesEvictThreshold int64

	// CommandTimeout for each command request.
	// Default: 1s
	CommandTimeout time.Duration

	// MaxConcurrency of commands that can be run on the data storage.
	// Default: 1 (disables use of worker pool)
	MaxConcurrency int

	// EvictionFrequency time between iterations of checking for evicted keys.
	// Default: 30s
	EvictionFrequency time.Duration

	// ExpirationFrequency time between iterations of checking for expired keys and freeing their memory.
	// Default: 30s
	ExpirationFrequency time.Duration
}

type Datkey struct {
	waitForEvictionWorker <-chan struct{}
	waitForExpireWorker   <-chan struct{}

	cache      cacheStorage
	cancelFunc context.CancelFunc
	config     Config
}

func New(config Config) *Datkey {
	if config.CommandTimeout == 0 {
		config.CommandTimeout = time.Second
	}

	if config.EvictStrategy == "" {
		config.EvictStrategy = EvictByLRU
	}

	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = 1
	}

	if config.EvictionFrequency == 0 {
		config.EvictionFrequency = 30 * time.Second //nolint:mnd // reason: default value
	}

	if config.ExpirationFrequency == 0 {
		config.ExpirationFrequency = 30 * time.Second //nolint:mnd // reason: default value
	}

	cache := newCacheStorage(config.MaxConcurrency)

	ctx, cancel := context.WithCancel(context.Background())

	return &Datkey{
		config:                config,
		cache:                 cache,
		cancelFunc:            cancel,
		waitForEvictionWorker: startEvictionWorker(ctx, config, cache),
		waitForExpireWorker:   startExpireWorker(ctx, config, cache),
	}
}

func (self *Datkey) Close() {
	self.cancelFunc()
}

// Set a key in the database.
// If ttl=0, then the key will never expire.
func (self *Datkey) Set(key string, value []byte, ttl time.Duration) SetResponse {
	return setKey(key, value, ttl, self.cache)
}

// Delete a key in the database.
func (self *Datkey) Delete(key string) DeleteResponse {
	return deleteKey(key, self.cache)
}

// Get a key from the database.
func (self *Datkey) Get(key string) GetResponse {
	return getKey(key, self.cache)
}

// Expire a key in the database in a given TTL.
func (self *Datkey) Expire(key string, ttl time.Duration) ExpireResponse {
	return expireKey(key, ttl, self.cache)
}

// Persist a key in the database by removing any TTL.
func (self *Datkey) Persist(key string) PersistResponse {
	return persistKey(key, self.cache)
}

// Ttl value of a key in the database.
func (self *Datkey) Ttl(key string) TtlResponse {
	return ttlKey(key, self.cache)
}

// Ping the database.
func (_ *Datkey) Ping() *errors.Error[DbReadErr] {
	return nil
}

func (self *Datkey) Stats() StatsResponse {
	return getDbStats(self.cache)
}

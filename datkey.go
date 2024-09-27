package datkey

import (
	"context"
	"time"

	"datkey/lib/errors"
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
}

type Datkey struct {
	slotCommandInput      []chan<- command
	waitForEvictionWorker <-chan struct{}
	waitForExpireWorker   <-chan struct{}
	cancelFunc            context.CancelFunc
	config                Config
}

func New(config Config) *Datkey {
	if config.CommandTimeout == 0 {
		config.CommandTimeout = time.Second
	}

	if config.EvictStrategy == "" {
		config.EvictStrategy = EvictByLRU
	}

	ctx, cancel := context.WithCancel(context.Background())

	slotCommandInput := startSlotWorkers(config)

	return &Datkey{
		config:                config,
		slotCommandInput:      slotCommandInput,
		cancelFunc:            cancel,
		waitForEvictionWorker: startEvictionWorker(ctx, config, slotCommandInput),
		waitForExpireWorker:   startExpireWorker(ctx, config, slotCommandInput),
	}
}

func (self *Datkey) Close() {
	self.cancelFunc()

	<-self.waitForEvictionWorker
	<-self.waitForExpireWorker

	for index := range self.slotCommandInput {
		close(self.slotCommandInput[index])
	}
}

// Set a key in the database.
// If ttl=0, then the key will never expire.
func (self *Datkey) Set(key string, value []byte, ttl time.Duration) (SetResponse, *errors.Error[DbWriteErr]) {
	return setKey(key, value, ttl, self.slotCommandInput, self.config.CommandTimeout)
}

// Delete a key in the database.
func (self *Datkey) Delete(key string) (DeleteResponse, *errors.Error[DbWriteErr]) {
	return deleteKey(key, self.slotCommandInput, self.config.CommandTimeout)
}

// Get a key from the database.
func (self *Datkey) Get(key string) (GetResponse, *errors.Error[DbReadErr]) {
	return getKey(key, self.slotCommandInput, self.config.CommandTimeout)
}

// Expire a key in the database in a given TTL.
func (self *Datkey) Expire(key string, ttl time.Duration) (ExpireResponse, *errors.Error[DbWriteErr]) {
	return expireKey(key, ttl, self.slotCommandInput, self.config.CommandTimeout)
}

// Persist a key in the database by removing any TTL.
func (self *Datkey) Persist(key string) (PersistResponse, *errors.Error[DbWriteErr]) {
	return persistKey(key, self.slotCommandInput, self.config.CommandTimeout)
}

// Ttl value of a key in the database.
func (self *Datkey) Ttl(key string) (TtlResponse, *errors.Error[DbReadErr]) {
	return ttlKey(key, self.slotCommandInput, self.config.CommandTimeout)
}

func (self *Datkey) Stats() (StatsResponse, *errors.Error[DbReadErr]) {
	return getDbStats(self.slotCommandInput, self.config.CommandTimeout)
}

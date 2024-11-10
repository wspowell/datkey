package datkey

import (
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond"

	"github.com/wspowell/datkey/hash"
)

type cacheStorage struct {
	workerPool *pond.WorkerPool
	slots      []*slotStorage
}

func newCacheStorage(maxConcurrency int) cacheStorage {
	hashSlotStorage := make([]*slotStorage, hash.MaxHashSlot)
	for index := range hashSlotStorage {
		hashSlotStorage[index] = &slotStorage{
			sizeInBytes: 0,
			storage:     map[string]keyStorage{},
			mutex:       sync.Mutex{},
		}
	}

	var workerPool *pond.WorkerPool
	if maxConcurrency > 1 {
		maxWorkers := maxConcurrency
		maxTaskCapacity := maxConcurrency
		workerPool = pond.New(maxWorkers, maxTaskCapacity, pond.Strategy(pond.Eager()))
	}

	return cacheStorage{
		workerPool: workerPool,
		slots:      hashSlotStorage,
	}
}

func (self cacheStorage) runCommand(hashSlot hash.Slot, cmd command) {
	if self.workerPool != nil {
		self.workerPool.SubmitAndWait(func() {
			hashSlotStorage := self.slots[hashSlot]
			hashSlotStorage.processCommand(cmd)
		})
	} else {
		hashSlotStorage := self.slots[hashSlot]
		hashSlotStorage.processCommand(cmd)
	}
}

type slotStorage struct {
	storage     map[string]keyStorage
	mutex       sync.Mutex
	sizeInBytes int64
}

func (self *slotStorage) processCommand(command command) {
	self.mutex.Lock()

	switch cmd := command.(type) {
	case commandSet:
		previousData, exists := self.storage[cmd.Key]
		self.sizeInBytes += int64(-len(previousData.value))
		if previousData.isExpired() {
			exists = false
			previousData.value = nil
		}
		self.storage[cmd.Key] = cmd.data
		self.sizeInBytes += int64(len(cmd.data.value))
		cmd.Resp.Exists = exists
		cmd.Resp.Value = previousData.value

		self.mutex.Unlock()
	case commandGet:
		data, exists := self.storage[cmd.Key]
		if data.isExpired() {
			self.sizeInBytes += int64(-len(data.value))
			exists = false
			data.value = nil
			delete(self.storage, cmd.Key)
		} else if exists {
			data.lastAccessTime = time.Now()
			self.storage[cmd.Key] = data
		}
		cmd.Resp.Exists = exists
		cmd.Resp.Value = data.value

		self.mutex.Unlock()
	case commandDelete:
		self.handleCommandDelete(cmd)

		self.mutex.Unlock()
	case commandExpire:
		previousData, exists := self.storage[cmd.Key]
		if previousData.isExpired() {
			self.sizeInBytes += int64(-len(previousData.value))
			exists = false
			previousData.value = nil
			delete(self.storage, cmd.Key)
		} else if exists {
			previousData.expiresAt = cmd.ExpiresAt
			self.storage[cmd.Key] = previousData
		}
		cmd.Resp.Exists = exists
		cmd.Resp.Value = previousData.value

		self.mutex.Unlock()
	case commandPersist:
		previousData, exists := self.storage[cmd.Key]
		if previousData.isExpired() {
			self.sizeInBytes += int64(-len(previousData.value))
			exists = false
			delete(self.storage, cmd.Key)
		} else if exists {
			previousData.expiresAt = time.Time{}
			self.storage[cmd.Key] = previousData
		}
		cmd.Resp.Exists = exists
		cmd.Resp.Value = previousData.value

		self.mutex.Unlock()
	case commandTtl:
		previousData, exists := self.storage[cmd.Key]
		var ttl time.Duration
		if previousData.isExpired() {
			self.sizeInBytes += int64(-len(previousData.value))
			exists = false
			delete(self.storage, cmd.Key)
		} else if exists && !previousData.expiresAt.IsZero() {
			ttl = time.Until(previousData.expiresAt)
		}
		cmd.Resp.Exists = exists
		cmd.Resp.Ttl = ttl

		self.mutex.Unlock()
	case commandPing:
		self.mutex.Unlock()
	case commandStats:
		cmd.Resp.sizeInBytes = self.sizeInBytes

		self.mutex.Unlock()
	case commandDeleteExpired:
		for key := range self.storage {
			// Prune expired keys.
			// TODO: This could be non-performant for large caches and might need to be works a bit smarter with sampling or other strategy.
			if self.storage[key].isExpired() {
				self.sizeInBytes += int64(-len(self.storage[key].value))
				delete(self.storage, key)
			}
		}
		cmd.Resp.Exists = false
		cmd.Resp.Value = nil

		self.mutex.Unlock()
	case commandDeleteLru:
		var lruKey string
		var lruAccessTime time.Time
		for key := range self.storage {
			if lruAccessTime.IsZero() || self.storage[key].lastAccessTime.Before(lruAccessTime) {
				lruKey = key
				lruAccessTime = self.storage[key].lastAccessTime
			}
		}
		self.handleCommandDelete(commandDelete{
			Key:  lruKey,
			Resp: cmd.Resp,
		})

		self.mutex.Unlock()
	default:
		self.mutex.Unlock()

		// This should never be hit and would indicate an internal library issue, so trigger a panic.
		panic(fmt.Sprintf("unexpected command type: %T, %+v", command, command))
	}
}

func (self *slotStorage) handleCommandDelete(cmd commandDelete) {
	previousData, exists := self.storage[cmd.Key]
	self.sizeInBytes += int64(-len(previousData.value))
	if previousData.isExpired() {
		exists = false
		previousData.value = nil
	}
	delete(self.storage, cmd.Key)
	cmd.Resp.Exists = exists
	cmd.Resp.Value = previousData.value
}

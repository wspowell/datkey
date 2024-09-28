package datkey

import (
	"fmt"
	"time"

	"github.com/wspowell/datkey/hash"
)

func startSlotWorkers(config Config) []chan<- command {
	slotCommandInput := make([]chan<- command, hash.MaxHashSlot)

	for slot := range hash.MaxHashSlot {
		commandChannel := make(chan command)
		slotCommandInput[slot] = commandChannel

		go slotWorker(commandChannel)

		resp := newResponse[struct{}]()

		go func(slotCommands chan<- command, resp *response[struct{}]) {
			slotCommands <- commandPing{
				Resp: resp,
			}
		}(commandChannel, resp)

		if _, err := resp.await(config.CommandTimeout); err != nil {
			panic("failed to start slot workers")
		}
	}

	return slotCommandInput
}

type slotCache struct {
	storage     map[string]keyStorage
	sizeInBytes int64
}

func slotWorker(commandChannel <-chan command) {
	cache := &slotCache{
		sizeInBytes: 0,
		storage:     map[string]keyStorage{},
	}

	for command := range commandChannel {
		commandHandler(cache, command)
	}
}

func commandHandler(cache *slotCache, command command) {
	switch cmd := command.(type) {
	case commandSet:
		previousData, exists := cache.storage[cmd.Key]
		cache.sizeInBytes += int64(-len(previousData.value))
		if previousData.isExpired() {
			exists = false
			previousData.value = nil
		}
		cache.storage[cmd.Key] = cmd.data
		cache.sizeInBytes += int64(len(cmd.data.value))
		cmd.Resp.send(valueResponse{
			Exists: exists,
			Value:  previousData.value,
		})
	case commandGet:
		data, exists := cache.storage[cmd.Key]
		if data.isExpired() {
			cache.sizeInBytes += int64(-len(data.value))
			exists = false
			data.value = nil
			delete(cache.storage, cmd.Key)
		} else {
			data.lastAccessTime = time.Now()
			cache.storage[cmd.Key] = data
		}
		cmd.Resp.send(valueResponse{
			Exists: exists,
			Value:  data.value,
		})
	case commandDelete:
		handleCommandDelete(cache, cmd)
	case commandExpire:
		previousData, exists := cache.storage[cmd.Key]
		if previousData.isExpired() {
			cache.sizeInBytes += int64(-len(previousData.value))
			exists = false
			previousData.value = nil
			delete(cache.storage, cmd.Key)
		} else {
			previousData.expiresAt = cmd.ExpiresAt
			cache.storage[cmd.Key] = previousData
		}
		cmd.Resp.send(valueResponse{
			Exists: exists,
			Value:  previousData.value,
		})
	case commandPersist:
		previousData, exists := cache.storage[cmd.Key]
		if previousData.isExpired() {
			cache.sizeInBytes += int64(-len(previousData.value))
			exists = false
			delete(cache.storage, cmd.Key)
		} else {
			previousData.expiresAt = time.Time{}
			cache.storage[cmd.Key] = previousData
		}
		cmd.Resp.send(valueResponse{
			Exists: exists,
			Value:  previousData.value,
		})
	case commandTtl:
		previousData, exists := cache.storage[cmd.Key]
		var ttl time.Duration
		if previousData.isExpired() {
			cache.sizeInBytes += int64(-len(previousData.value))
			exists = false
			delete(cache.storage, cmd.Key)
		} else if !previousData.expiresAt.IsZero() {
			ttl = time.Until(previousData.expiresAt)
		}
		cmd.Resp.send(ttlResponse{
			Exists: exists,
			Ttl:    ttl,
		})
	case commandPing:
		cmd.Resp.send(struct{}{})
	case commandStats:
		cmd.Resp.send(statsResponse{
			sizeInBytes: cache.sizeInBytes,
		})
	case commandDeleteExpired:
		for key := range cache.storage {
			// Prune expired keys.
			// TODO: This could be non-performant for large caches and might need to be works a bit smarter with sampling or other strategy.
			if cache.storage[key].isExpired() {
				cache.sizeInBytes += int64(-len(cache.storage[key].value))
				delete(cache.storage, key)
			}
		}
		cmd.Resp.send(valueResponse{
			Exists: false,
			Value:  nil,
		})
	case commandDeleteLru:
		var lruKey string
		var lruAccessTime time.Time
		for key := range cache.storage {
			if lruAccessTime.IsZero() || cache.storage[key].lastAccessTime.Before(lruAccessTime) {
				lruKey = key
				lruAccessTime = cache.storage[key].lastAccessTime
			}
		}
		handleCommandDelete(cache, commandDelete{
			Key:  lruKey,
			Resp: cmd.Resp,
		})
	default:
		// This should never be hit and would indicate an internal library issue, so trigger a panic.
		panic(fmt.Sprintf("unexpected command type: %T, %+v", command, command))
	}
}

func handleCommandDelete(cache *slotCache, cmd commandDelete) {
	previousData, exists := cache.storage[cmd.Key]
	cache.sizeInBytes += int64(-len(previousData.value))
	if previousData.isExpired() {
		exists = false
		previousData.value = nil
	}
	delete(cache.storage, cmd.Key)
	cmd.Resp.send(valueResponse{
		Exists: exists,
		Value:  previousData.value,
	})
}

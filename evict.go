package datkey

import (
	"context"
	"fmt"
	"time"

	"github.com/wspowell/datkey/hash"
)

func startEvictionWorker(ctx context.Context, config Config, cache cacheStorage) <-chan struct{} {
	if config.DbBytesEvictThreshold == 0 {
		// Do not run any eviction worker.
		done := make(chan struct{})
		close(done)
		return done
	}

	switch config.EvictStrategy {
	case EvictByLRU:
		return lruEviction(ctx, config, cache)
	case EvictByTTL:
		panic("EvictByTTL is not implemented")
	case EvictDisabled:
		// Do not run any eviction worker.
		done := make(chan struct{})
		close(done)
		return done
	default:
		panic(fmt.Sprintf("invalid eviction strategy: %s", config.EvictStrategy))
	}
}

func startExpireWorker(ctx context.Context, config Config, cache cacheStorage) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		var nextHashSlot hash.Slot
		for {
			if nextHashSlot == 0 {
				// TODO: Instead of sleeping and repeatedly checking db stats, there has to be a better and more reactive way of handling this.
				time.Sleep(config.ExpirationFrequency)
			}

			nextHashSlot++
			if nextHashSlot >= hash.MaxHashSlot {
				nextHashSlot = 0
			}

			select {
			case <-ctx.Done():
				close(done)
				return
			default:
				deleteExpired(nextHashSlot, cache)
			}
		}
	}()

	return done
}

func lruEviction(ctx context.Context, config Config, cache cacheStorage) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		for {
			// TODO: Instead of sleeping and repeatedly checking db stats, there has to be a better and more reactive way of handling this.
			time.Sleep(config.EvictionFrequency)

			select {
			case <-ctx.Done():
				close(done)
				return
			default:
				dbStats := getDbStats(cache)
				if dbStats.DbSizeInBytes > config.DbBytesEvictThreshold {
					deleteLru(cache)
					continue
				}
			}
		}
	}()

	return done
}

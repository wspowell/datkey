package datkey

import (
	"context"
	"fmt"
	"time"

	"datkey/hash"
)

func startEvictionWorker(ctx context.Context, config Config, slotCommandInput []chan<- command) <-chan struct{} {
	if config.DbBytesEvictThreshold == 0 {
		// Do not run any eviction worker.
		done := make(chan struct{})
		close(done)
		return done
	}

	switch config.EvictStrategy {
	case EvictByLRU:
		return lruEviction(ctx, config, slotCommandInput)
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

func startExpireWorker(ctx context.Context, config Config, slotCommandInput []chan<- command) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		var nextHashSlot hash.Slot
		for {
			nextHashSlot++
			if nextHashSlot >= hash.MaxHashSlot {
				nextHashSlot = 0
			}

			select {
			case <-ctx.Done():
				close(done)
				return
			default:
				if err := deleteExpired(nextHashSlot, slotCommandInput, config.CommandTimeout); err != nil {
					// Handle error?
					break // from select
				}
				continue
			}

			if nextHashSlot == 0 {
				// TODO: Instead of sleeping and repeatedly checking db stats, there has to be a better and more reactive way of handling this.
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return done
}

func lruEviction(ctx context.Context, config Config, slotCommandInput []chan<- command) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(done)
				return
			default:
				dbStats, err := getDbStats(slotCommandInput, config.CommandTimeout)
				if err != nil {
					// Handle error?
					break // from select
				}
				if dbStats.DbSizeInBytes > config.DbBytesEvictThreshold {
					if err := deleteLru(slotCommandInput, config.CommandTimeout); err != nil {
						// Handle error?
						break // from select
					}
					continue
				}
			}

			// TODO: Instead of sleeping and repeatedly checking db stats, there has to be a better and more reactive way of handling this.
			time.Sleep(1 * time.Second)
		}
	}()

	return done
}

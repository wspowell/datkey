package datkey_test

import (
	"testing"
	"time"

	"github.com/hashicorp/go-uuid"

	"github.com/wspowell/datkey"
)

//nolint:gochecknoglobals // reason: Preloads this slice for all bench tests.
var guids []string

//nolint:gochecknoinits // reason: Preloads this slice for all bench tests.
func init() {
	guids = generateGuids()
}

// Generate guids to avoid impacting benchmarks with guid generation.
func generateGuids() []string {
	ids := make([]string, 10000000)
	for index := range ids {
		id, _ := uuid.GenerateUUID()
		ids[index] = id
	}
	return ids
}

func BenchmarkDatKeySet_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = client.Set("test", data, 0)
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = client.Set("test", data, 0)
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_ = client.Set(guids[idIndex], data, 0)
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_ = client.Set(guids[idIndex], data, 0)
			idIndex++
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_ = client.Set("test", []byte("value"), 0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = client.Get("test")
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_ = client.Set("test", []byte("value"), 0)

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = client.Get("test")
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	for index := range guids {
		_ = client.Set(guids[index], []byte("value"), 0)
	}

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_ = client.Get(guids[idIndex])
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	for index := range guids {
		_ = client.Set(guids[index], []byte("value"), 0)
	}

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_ = client.Get(guids[idIndex])
			idIndex++
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeySet_sync_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = client.Set("test", data, time.Second)
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_async_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = client.Set("test", data, time.Second)
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_sync_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_ = client.Set(guids[idIndex], data, time.Second)
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_async_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	data := []byte("value")

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_ = client.Set(guids[idIndex], data, time.Second)
			idIndex++
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_sync_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_ = client.Set("test", []byte("value"), time.Second)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = client.Get("test")
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_async_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_ = client.Set("test", []byte("value"), time.Second)

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = client.Get("test")
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_sync_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	for index := range guids {
		_ = client.Set(guids[index], []byte("value"), time.Second)
	}

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_ = client.Get(guids[idIndex])
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_async_with_TTL(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	for index := range guids {
		_ = client.Set(guids[index], []byte("value"), time.Second)
	}

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_ = client.Get(guids[idIndex])
			idIndex++
		}
	})

	b.StopTimer()
}

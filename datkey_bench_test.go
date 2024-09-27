package datkey_test

import (
	"testing"

	"github.com/hashicorp/go-uuid"

	"datkey"
)

// Generate guids to avoid impacting benchmarks with guid generation.
func generateGuids() []string {
	ids := make([]string, 1000000)
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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.Set("test", []byte("value"), 0)
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_, err := client.Set("test", []byte("value"), 0)
			if err != nil {
				panic(err)
			}
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	guids := generateGuids()

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_, err := client.Set(guids[idIndex], []byte("value"), 0)
		if err != nil {
			panic(err)
		}
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeySet_multikey_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	guids := generateGuids()

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_, err := client.Set(guids[idIndex], []byte("value"), 0)
			if err != nil {
				panic(err)
			}
			idIndex++
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_, err := client.Set("test", []byte("value"), 0)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.Get("test")
		if err != nil {
			panic(err)
		}
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	_, err := client.Set("test", []byte("value"), 0)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_, err := client.Get("test")
			if err != nil {
				panic(err)
			}
		}
	})

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_sync(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	guids := generateGuids()

	for index := range guids {
		_, err := client.Set(guids[index], []byte("value"), 0)
		if err != nil {
			panic(err)
		}
	}

	b.ResetTimer()

	var idIndex int
	for i := 0; i < b.N; i++ {
		_, err := client.Get(guids[idIndex])
		if err != nil {
			panic(err)
		}
		idIndex++
	}

	b.StopTimer()
}

func BenchmarkDatKeyGet_multikey_async(b *testing.B) {
	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	guids := generateGuids()

	for index := range guids {
		_, err := client.Set(guids[index], []byte("value"), 0)
		if err != nil {
			panic(err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		var idIndex int
		for p.Next() {
			_, err := client.Get(guids[idIndex])
			if err != nil {
				panic(err)
			}
			idIndex++
		}
	})

	b.StopTimer()
}

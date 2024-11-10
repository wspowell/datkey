package datkey_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wspowell/datkey"
)

func TestDatkey_Ping(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	{
		err := client.Ping()
		assert.Nil(t, err)
	}
}

func TestDatkey_Set_Get(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	{
		result := client.Set("test", []byte("value"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Get("test")
		assert.True(t, result.Exists)
		assert.Equal(t, []byte("value"), result.Value)
	}
}

func TestDatkey_Set_ttl_Get(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Get("test")
		assert.True(t, result.Exists)
		assert.Equal(t, []byte("value"), result.Value)
	}
}

func TestDatkey_Set_ttl_Expire(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(ttl)

	{
		result := client.Expire("test", ttl)
		assert.False(t, result.Exists)
	}
}

func TestDatkey_Set_ttl_Expire_race(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	value := []byte("value")
	ttl := time.Second
	{
		result := client.Set("test", value, ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	done := time.After(ttl)
	for {
		select {
		case <-done:
			return
		default:
			result := client.Get("test")
			if result.Exists {
				assert.Equal(t, value, result.Value)
			} else {
				assert.Nil(t, result.Value)
			}
		}
	}
}

func TestDatkey_Set_ttl_Get_expired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Wait until the key is expired.
	time.Sleep(ttl)

	{
		result := client.Get("test")
		assert.False(t, result.Exists)
		assert.Nil(t, result.Value)
	}
}

func TestDatkey_Set_Expire_Get_expired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Expire("test", ttl)
		assert.True(t, result.Exists)
	}

	{
		result := client.Get("test")
		assert.True(t, result.Exists)
		assert.Equal(t, []byte("value"), result.Value)
	}

	// Wait until the key is expired.
	time.Sleep(ttl)

	{
		result := client.Get("test")
		assert.False(t, result.Exists)
		assert.Nil(t, result.Value)
	}
}

func TestDatkey_Set_Expire0_Get_expired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	{
		result := client.Set("test", []byte("value"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Expire("test", 0)
		assert.True(t, result.Exists)
	}

	{
		result := client.Get("test")
		assert.False(t, result.Exists)
		assert.Nil(t, result.Value)
	}
}

func TestDatkey_Set_ttl_Set_expired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(ttl)

	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}
}

func TestDatkey_Set_ttl_Persist(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Persist("test")
		assert.True(t, result.Exists)
	}

	time.Sleep(ttl)

	{
		result := client.Get("test")
		assert.True(t, result.Exists)
	}
}

func TestDatkey_Set_ttl_Persist_Ttl(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result := client.Set("test", []byte("value"), ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Ttl("test")
		assert.True(t, result.Exists)
		assert.NotZero(t, result.Ttl)
	}

	{
		result := client.Persist("test")
		assert.True(t, result.Exists)
	}

	{
		result := client.Ttl("test")
		assert.True(t, result.Exists)
		assert.Zero(t, result.Ttl)
	}

	{
		result := client.Expire("test", time.Second)
		assert.True(t, result.Exists)
	}

	time.Sleep(time.Second)

	{
		result := client.Ttl("test")
		assert.False(t, result.Exists)
		assert.Zero(t, result.Ttl)
	}
}

func TestDatkey_Delete(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	{
		result := client.Delete("test")
		assert.False(t, result.Exists)
	}

	{
		result := client.Set("test", []byte("value"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Delete("test")
		assert.True(t, result.Exists)
	}

	{
		result := client.Delete("test")
		assert.False(t, result.Exists)
	}

	{
		result := client.Set("test", []byte("value"), time.Second)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(time.Second)

	{
		result := client.Delete("test")
		assert.False(t, result.Exists)
	}
}

func TestDatkey_deleteExpired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	config.ExpirationFrequency = time.Second
	client := datkey.New(config)
	defer client.Close()

	value := []byte("value")
	ttl := time.Second
	{
		result := client.Set("test", value, ttl)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result := client.Stats()
		assert.Equal(t, int64(len(value)), result.DbSizeInBytes)
	}

	time.Sleep(ttl + 5*time.Second)

	{
		result := client.Stats()
		assert.Zero(t, result.DbSizeInBytes)
	}
}

func TestDatkey_Stats(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	var expectedDbSizeBytes int64

	{
		result := client.Stats()
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}

	{
		value := []byte("value")
		expectedDbSizeBytes += int64(len(value))
		_ = client.Set("test", value, 0)
	}

	{
		result := client.Stats()
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}

	{
		value := []byte("updatedValue")
		expectedDbSizeBytes += int64(len(value) - len([]byte("value")))
		_ = client.Set("test", value, 0)
	}

	{
		result := client.Stats()
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}
}

func TestDatkey_No_Eviction(t *testing.T) {
	t.Parallel()

	config := datkey.Config{
		EvictStrategy:         datkey.EvictDisabled,
		DbBytesEvictThreshold: 50,
		CommandTimeout:        time.Second,
		MaxConcurrency:        1,
		EvictionFrequency:     time.Second,
		ExpirationFrequency:   time.Second,
	}
	client := datkey.New(config)
	defer client.Close()

	for i := range 10 {
		result := client.Set(strconv.Itoa(i), []byte("1234567890"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Give the LRU worker some time to process.
	time.Sleep(5 * time.Second)

	result := client.Stats()
	assert.Equal(t, int64(100), result.DbSizeInBytes)
}

func TestDatkey_LRU_Eviction(t *testing.T) {
	t.Parallel()

	config := datkey.Config{
		EvictStrategy:         datkey.EvictByLRU,
		DbBytesEvictThreshold: 50,
		CommandTimeout:        time.Second,
		MaxConcurrency:        1,
		EvictionFrequency:     time.Second,
		ExpirationFrequency:   time.Second,
	}
	client := datkey.New(config)
	defer client.Close()

	for i := range 10 {
		result := client.Set(strconv.Itoa(i), []byte("1234567890"), 0)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Give the LRU worker some time to process.
	time.Sleep(config.EvictionFrequency * 2)

	result := client.Stats()
	assert.LessOrEqual(t, result.DbSizeInBytes, config.DbBytesEvictThreshold)
}

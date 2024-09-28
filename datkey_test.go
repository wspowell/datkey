package datkey_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wspowell/datkey"
)

func TestDatkey_Set_Get(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	{
		result, err := client.Set("test", []byte("value"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(ttl)

	{
		result, err := client.Expire("test", ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
	}
}

func TestDatkey_Set_ttl_Get_expired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	ttl := time.Second
	{
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Wait until the key is expired.
	time.Sleep(ttl)

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Expire("test", ttl)
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
		assert.Equal(t, []byte("value"), result.Value)
	}

	// Wait until the key is expired.
	time.Sleep(ttl)

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Expire("test", 0)
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(ttl)

	{
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Persist("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	time.Sleep(ttl)

	{
		result, err := client.Get("test")
		assert.Nil(t, err)
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
		result, err := client.Set("test", []byte("value"), ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Ttl("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
		assert.NotZero(t, result.Ttl)
	}

	{
		result, err := client.Persist("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	{
		result, err := client.Ttl("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
		assert.Zero(t, result.Ttl)
	}

	{
		result, err := client.Expire("test", time.Second)
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	time.Sleep(time.Second)

	{
		result, err := client.Ttl("test")
		assert.Nil(t, err)
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
		result, err := client.Delete("test")
		assert.Nil(t, err)
		assert.False(t, result.Exists)
	}

	{
		result, err := client.Set("test", []byte("value"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Delete("test")
		assert.Nil(t, err)
		assert.True(t, result.Exists)
	}

	{
		result, err := client.Delete("test")
		assert.Nil(t, err)
		assert.False(t, result.Exists)
	}

	{
		result, err := client.Set("test", []byte("value"), time.Second)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	time.Sleep(time.Second)

	{
		result, err := client.Delete("test")
		assert.Nil(t, err)
		assert.False(t, result.Exists)
	}
}

func TestDatkey_deleteExpired(t *testing.T) {
	t.Parallel()

	var config datkey.Config
	client := datkey.New(config)
	defer client.Close()

	value := []byte("value")
	ttl := time.Second
	{
		result, err := client.Set("test", value, ttl)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	{
		result, err := client.Stats()
		assert.Nil(t, err)
		assert.Equal(t, int64(len(value)), result.DbSizeInBytes)
	}

	time.Sleep(ttl + 5*time.Second)

	{
		result, err := client.Stats()
		assert.Nil(t, err)
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
		result, err := client.Stats()
		assert.Nil(t, err)
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}

	{
		value := []byte("value")
		expectedDbSizeBytes += int64(len(value))
		_, err := client.Set("test", value, 0)
		assert.Nil(t, err)
	}

	{
		result, err := client.Stats()
		assert.Nil(t, err)
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}

	{
		value := []byte("updatedValue")
		expectedDbSizeBytes += int64(len(value) - len([]byte("value")))
		_, err := client.Set("test", value, 0)
		assert.Nil(t, err)
	}

	{
		result, err := client.Stats()
		assert.Nil(t, err)
		assert.Equal(t, expectedDbSizeBytes, result.DbSizeInBytes)
	}
}

func TestDatkey_No_Eviction(t *testing.T) {
	t.Parallel()

	config := datkey.Config{
		EvictStrategy:         datkey.EvictDisabled,
		DbBytesEvictThreshold: 50,
		CommandTimeout:        time.Second,
	}
	client := datkey.New(config)
	defer client.Close()

	for i := range 10 {
		result, err := client.Set(strconv.Itoa(i), []byte("1234567890"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Give the LRU worker some time to process.
	time.Sleep(5 * time.Second)

	result, err := client.Stats()
	assert.Nil(t, err)
	assert.Equal(t, int64(100), result.DbSizeInBytes)
}

func TestDatkey_LRU_Eviction(t *testing.T) {
	t.Parallel()

	config := datkey.Config{
		EvictStrategy:         datkey.EvictByLRU,
		DbBytesEvictThreshold: 50,
		CommandTimeout:        time.Second,
	}
	client := datkey.New(config)
	defer client.Close()

	for i := range 10 {
		result, err := client.Set(strconv.Itoa(i), []byte("1234567890"), 0)
		assert.Nil(t, err)
		assert.False(t, result.Exists)
		assert.Nil(t, result.PreviousValue)
	}

	// Give the LRU worker some time to process.
	time.Sleep(5 * time.Second)

	result, err := client.Stats()
	assert.Nil(t, err)
	assert.LessOrEqual(t, result.DbSizeInBytes, config.DbBytesEvictThreshold)
}

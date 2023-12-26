package mutex

import (
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRWMutex_WithTries(t *testing.T) {
	rw := NewRWMutex(WithTries(5))
	assert.Equal(t, 5, rw.tries)
}

func TestRWMutex_WithRetryInterval(t *testing.T) {
	rw := NewRWMutex(WithRetryInterval(time.Second * 5))
	assert.Equal(t, time.Second*5, rw.retryInterval)
}

func TestRWMutex_WithTimeout(t *testing.T) {
	rw := NewRWMutex(WithTimeout(time.Second * 10))
	assert.Equal(t, time.Second*10, rw.timeout)
}

func TestRWMutex_WithClient(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	rw := NewRWMutex(WithClient(client))
	assert.Equal(t, client, rw.client)
}

func TestRWMutex_WithGenValueFunc(t *testing.T) {
	genValueFunc := func() (string, error) {
		return "test", nil
	}
	rw := NewRWMutex(WithGenValueFunc(genValueFunc))
	value, _ := rw.genValueFunc()
	assert.Equal(t, "test", value)
}

func TestRWMutex_WithName(t *testing.T) {
	rw := NewRWMutex(WithName("test"))
	assert.Equal(t, "test", rw.name)
}

func TestRWMutex_WithExpiry(t *testing.T) {
	rw := NewRWMutex(WithExpiry(time.Second * 5))
	assert.Equal(t, time.Second*5, rw.expiry)
}

func TestRWMutex_ReadLock(t *testing.T) {
	rw := NewRWMutex()
	readLock := rw.ReadLock()
	assert.NotNil(t, readLock)
}

func TestRWMutex_WriteLock(t *testing.T) {
	rw := NewRWMutex()
	writeLock := rw.WriteLock()
	assert.NotNil(t, writeLock)
}

func TestRWMutex_Value(t *testing.T) {
	rw := NewRWMutex()
	assert.Equal(t, "", rw.Value())
}

func TestRWMutex_WriteExclusive(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "testRWMutexExclusive"

	locker1 := &RWMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		genValueFunc:  genValue,
		client:        client,
	}

	locker2 := &RWMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		genValueFunc:  genValue,
		client:        client,
	}

	err := locker1.WriteLock().Lock()
	assert.NoError(t, err)

	err = locker2.ReadLock().Lock()
	assert.Error(t, err)

	err = locker1.ReadLock().Lock()
	assert.NoError(t, err)
}

func TestRWMutex_WriteExclusive2(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "testRWMutexExclusive"

	locker1 := &RWMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		genValueFunc:  genValue,
		client:        client,
	}

	locker2 := &RWMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		genValueFunc:  genValue,
		client:        client,
	}
	err := locker1.ReadLock().Lock()
	assert.NoError(t, err)

	// can lock
	err = locker2.ReadLock().Lock()
	assert.NoError(t, err)

	err = locker2.WriteLock().Lock()
	assert.Error(t, err)

	// can not lock, because other read lock is still active
	err = locker1.WriteLock().Lock()
	assert.Error(t, err)

}

package mutex

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"log"
	"sync"
	"testing"
	"time"
)

func TestWriteMutex_RunCorrectly(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "testWriteMutexRun"

	locker1 := &WriteMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}

	err := locker1.Lock()
	assert.NoError(t, err)

	i, err := client.HGet(context.Background(), lockKey, locker1.value).Int()
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	unlock, err := locker1.Unlock()
	assert.NoError(t, err)
	assert.False(t, unlock)
}

func TestWriteMutexExclusion(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "testWriteMutexExclusion"

	locker1 := &WriteMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		value:         "1",
	}
	locker2 := &WriteMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		value:         "2",
	}

	var wg sync.WaitGroup
	tryLock := func(locker *WriteMutex, shouldFail bool) {
		defer wg.Done()

		println("enter %s", locker.value)
		err := locker.Lock()
		if shouldFail {
			log.Printf("should failed, the err is %s \n", err)
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}

	wg.Add(2)
	go tryLock(locker1, false)
	time.Sleep(100 * time.Millisecond) // Ensure locker1 acquires the lock first
	go tryLock(locker2, true)          // This should fail as locker1 already has the lock
	wg.Wait()

	unlock, err := locker1.Unlock()
	assert.NoError(t, err)
	assert.False(t, unlock)

	wg.Wait()
}

func TestWriteMutexReentrancy(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "testWriteMutexReentrancy"
	locker := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
	}

	// Lock once
	err := locker.Lock()
	assert.NoError(t, err)
	i, err := client.HGet(context.Background(), lockKey, locker.value).Int()
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	// Lock again, should not fail due to reentrancy
	err = locker.Lock()
	assert.NoError(t, err)
	i, err = client.HGet(context.Background(), lockKey, locker.value).Int()
	assert.NoError(t, err)
	assert.Equal(t, 2, i)

	// Unlock once
	_, err = locker.Unlock()
	assert.NoError(t, err)
	i, err = client.HGet(context.Background(), lockKey, locker.value).Int()
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	// Unlock again, should not fail due to reentrancy
	_, err = locker.Unlock()
	assert.NoError(t, err)

	_, err = client.HGet(context.Background(), lockKey, locker.value).Int()
	assert.Equal(t, redis.Nil, err)
}

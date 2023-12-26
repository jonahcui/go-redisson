package mutex

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestLockAndUnlock(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	m := &ReadMutex{
		name:          "test1",
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}

	err := m.Lock()
	assert.NoError(t, err)

	t1, ttl1, err := getRwLockTimeout(client, "test1", m.value, 1)
	fmt.Printf("t1:%d, ttl1:%f\n", t1, ttl1)
	assert.NoError(t, err)
	assert.Equal(t, 1, t1)
	assert.Equal(t, true, ttl1 >= 90.0 && ttl1 <= 101.0)

	result, err := m.Unlock()
	assert.NoError(t, err)
	assert.Equal(t, false, result)
}

func TestReentrancy(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "test2"
	m := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}

	err := m.Lock()
	assert.NoError(t, err)

	err = m.Lock()
	assert.NoError(t, err)

	err = m.Lock()
	assert.NoError(t, err)

	t1, ttl1, err := getRwLockTimeout(client, lockKey, m.value, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, t1)
	assert.Equal(t, true, ttl1 > 90 && ttl1 < 101)

	t2, ttl2, err := getRwLockTimeout(client, lockKey, m.value, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, t2)
	assert.Equal(t, true, ttl2 > 90 && ttl2 < 101)

	t3, ttl3, err := getRwLockTimeout(client, lockKey, m.value, 3)
	assert.NoError(t, err)
	assert.Equal(t, 1, t3)
	assert.Equal(t, true, ttl3 > 90 && ttl3 < 101)

	lockMap, err := client.HGetAll(context.Background(), lockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lockMap))
	assert.Equal(t, "read", lockMap["mode"])
	assert.Equal(t, "3", lockMap[m.value])

	result, err := m.Unlock()
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	lockMap, err = client.HGetAll(context.Background(), lockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lockMap))
	assert.Equal(t, "read", lockMap["mode"])
	assert.Equal(t, "2", lockMap[m.value])

	result, err = m.Unlock()
	assert.NoError(t, err)
	assert.Equal(t, true, result)

	lockMap, err = client.HGetAll(context.Background(), lockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lockMap))
	assert.Equal(t, "read", lockMap["mode"])
	assert.Equal(t, "1", lockMap[m.value])

	result, err = m.Unlock()
	assert.NoError(t, err)
	assert.Equal(t, false, result)

	finalMap, err := client.HGetAll(context.Background(), lockKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(finalMap))
}

func TestMultipleRead(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockKey := "test2"

	locker1 := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}
	locker2 := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}
	locker3 := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}
	locker4 := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}
	locker5 := &ReadMutex{
		name:          lockKey,
		expiry:        100 * time.Second,
		tries:         3,
		retryInterval: 1 * time.Second,
		timeout:       5 * time.Second,
		client:        client,
		genValueFunc:  genValue,
	}

	var wg sync.WaitGroup
	tryLock := func(locker *ReadMutex) {
		defer wg.Done()

		err := locker.Lock()
		assert.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		_, err = locker.Unlock()
		assert.NoError(t, err)
	}

	wg.Add(5)
	startTime := time.Now().Unix()
	go tryLock(locker1)
	go tryLock(locker2)
	go tryLock(locker3)
	go tryLock(locker4)
	go tryLock(locker5)
	endTime := time.Now().Unix()

	wg.Wait()
	// concurrent(5* 500ms) < 1s
	assert.Equal(t, true, endTime-startTime < 1)

}

func getRwLockTimeout(client *redis.Client, key, value string, t uint) (int, float64, error) {
	cmd := client.Get(context.Background(), fmt.Sprintf("%s:%s:rwlock_timeout:%d", key, value, t))
	ttlCmd := client.TTL(context.Background(), fmt.Sprintf("%s:%s:rwlock_timeout:%d", key, value, t))
	result, err := cmd.Int()
	if err != nil {
		return result, 0, err
	}
	ttl, err := ttlCmd.Result()
	return result, ttl.Seconds(), err
}

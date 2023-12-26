package mutex

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

var (
	UnlockMessage     = 0
	ReadUnlockMessage = 1
)

// WriteMutex is a distributed mutual exclusion lock. It is similar to a ReadMutex but allows only one writer at a time.
type WriteMutex struct {
	name   string
	expiry time.Duration

	tries         int
	retryInterval time.Duration
	timeout       time.Duration

	genValueFunc        func() (string, error)
	value               string
	until               time.Time
	client              *redis.Client
	lockWatchdogTimeout time.Duration
}

// Name returns mutex name (i.e. the Redis key).
func (m *WriteMutex) Name() string {
	return m.name
}

// Value returns the current random value. The value will be empty until a lock is acquired (or WithValue option is used).
func (m *WriteMutex) Value() string {
	return m.value
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *WriteMutex) Until() time.Time {
	return m.until
}

// TryLock only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *WriteMutex) TryLock() error {
	return m.TryLockContext(context.Background())
}

// TryLockContext only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *WriteMutex) TryLockContext(ctx context.Context) error {
	return m.lockContext(ctx, 1)
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *WriteMutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *WriteMutex) LockContext(ctx context.Context) error {
	return m.lockContext(ctx, m.tries)
}

func (m *WriteMutex) lockContext(ctx context.Context, tries int) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	value := m.value
	if value == "" {
		value, err = m.genValueFunc()
		if err != nil {
			return err
		}
	}

	var timer *time.Timer
	for i := 0; i < tries; i++ {
		if i != 0 {
			if timer == nil {
				timer = time.NewTimer(m.retryInterval)
			} else {
				timer.Reset(m.retryInterval)
			}

			select {
			case <-ctx.Done():
				timer.Stop()
				// Exit early if the context is done.
				return ErrInterrupted
			case <-timer.C:
				// Fall-through when the delay timer completes.
			}
		}

		start := time.Now()

		result, err := func() (bool, error) {
			ctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			return m.acquire(ctx, value)
		}()

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start))
		if result && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		} else if result {
			_, err = m.Unlock()
		}

		if i == m.tries-1 && err != nil {
			return err
		}
	}

	return ErrLockFailed
}
func (m *WriteMutex) Unlock() (bool, error) {
	return m.UnlockContext(context.Background())
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *WriteMutex) UnlockContext(ctx context.Context) (bool, error) {
	requestId, err := genValue()
	if err != nil {
		return false, err
	}
	for i := 0; i < m.tries; i++ {
		result, err := m.release(ctx, m.value, requestId)
		if err != nil {
			continue
		}

		cnt, err := m.client.Del(ctx, m.getUnlockLatchName(m.name, m.value, requestId)).Result()
		if err != nil || cnt < 1 {
			log.Printf("del unlock latch failed, count: %d, err: %s", cnt, err)
		}
		return result, err
	}

	return true, nil
}

var acquireWriteScript = `
local mode = redis.call('hget', KEYS[1], 'mode'); 
if (mode == false) then 
redis.call('hset', KEYS[1], 'mode', 'write'); 
redis.call('hset', KEYS[1], ARGV[2], 1); 
redis.call('pexpire', KEYS[1], ARGV[1]); 
return nil; 
end; 
if (mode == 'write') then 
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then 
redis.call('hincrby', KEYS[1], ARGV[2], 1); 
local currentExpire = redis.call('pttl', KEYS[1]); 
redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); 
return nil; 
end; 
end;
return redis.call('pttl', KEYS[1]);
`

// acquire
func (m *WriteMutex) acquire(ctx context.Context, value string) (bool, error) {
	cmd := m.client.Eval(ctx, acquireWriteScript, []string{m.name}, m.expiry/time.Millisecond, value+":write")
	// how to check script's return value is nil?
	err := cmd.Err()

	if errors.Is(err, redis.Nil) {
		return true, nil
	}

	if err != nil {
		log.Printf("acquire write lock failed, err: %s", err.Error())
		return false, err
	}

	return false, nil
}

var releaseWriteScript = `
local val = redis.call('get', KEYS[3]); 
if val ~= false then 
return tonumber(val);
end; 
local mode = redis.call('hget', KEYS[1], 'mode'); 
if (mode == false) then 
redis.call(ARGV[4], KEYS[2], ARGV[1]); 
redis.call('set', KEYS[3], 1, 'px', ARGV[5]); 
return 1; 
end;
if (mode == 'write') then 
local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); 
if (lockExists == 0) then 
return nil;
else 
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); 
if (counter > 0) then 
redis.call('pexpire', KEYS[1], ARGV[2]); 
redis.call('set', KEYS[3], 0, 'px', ARGV[5]); 
return 0; 
else 
redis.call('hdel', KEYS[1], ARGV[3]); 
if (redis.call('hlen', KEYS[1]) == 1) then 
redis.call('del', KEYS[1]); 
redis.call(ARGV[4], KEYS[2], ARGV[1]); 
else 
redis.call('hset', KEYS[1], 'mode', 'read'); 
end; 
redis.call('set', KEYS[3], 1, 'px', ARGV[5]); 
return 1; 
end; 
end; 
end; 
return nil;
`

// release attempts to release the lock held by the WriteMutex.
// It uses a Lua script executed on the Redis server to atomically check the lock state and perform the necessary operations.
// The function returns two values:
// The first boolean return value indicates whether the lock is still held by someone else. If it's true, it means the lock is still held by someone else. If it's false, it means the lock has been successfully released.
// The second return value is an error which will be non-nil if an error occurred during the operation.
func (m *WriteMutex) release(ctx context.Context, value string, requestId string) (bool, error) {
	cmd := m.client.Eval(ctx, releaseWriteScript, []string{m.name,
		m.getChannelName(m.name),
		m.getUnlockLatchName(m.name, value+"write", requestId),
	}, ReadUnlockMessage, m.expiry.Milliseconds(), value+":write", "PUBLISH", m.expiry.Milliseconds())

	// script return nil, don't hold the key
	if errors.Is(cmd.Err(), redis.Nil) {
		return false, nil
	}

	result, err := cmd.Int()

	if err != nil {
		println("release write lock failed, err: ", err.Error())
		return false, err
	}
	// script return 0, other one has the lock
	if result == 0 {
		return true, nil
	}
	// script return 1, the lock doesn't exist
	if result == 1 {
		return false, nil
	}

	// script return greater than 1, the latch exists
	return false, nil
}

func (m *WriteMutex) getChannelName(key string) string {
	return "redisson_rwlock:" + key
}

func (m *WriteMutex) getUnlockLatchName(key, value, requestId string) string {
	return "redisson_unlock_latch:" + key + ":" + value + ":" + requestId
}

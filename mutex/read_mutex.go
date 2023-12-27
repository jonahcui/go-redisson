package mutex

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"math/rand"
	"time"
)

var (
	ErrInterrupted  = errors.New("mutex: interrupted when waiting to retry")
	ErrTimeout      = errors.New("mutex: timed out waiting for lock")
	ErrLockFailed   = errors.New("mutex: acquire lock failed")
	ErrLockConflict = errors.New("mutex: acquire lock conflict")
)

type ReadMutex struct {
	name   string
	expiry time.Duration

	tries         int
	retryInterval time.Duration
	timeout       time.Duration

	genValueFunc func() (string, error)
	value        string
	until        time.Time
	client       *redis.Client
}

// Name returns mutex name (i.e. the Redis key).
func (m *ReadMutex) Name() string {
	return m.name
}

// Value returns the current random value. The value will be empty until a lock is acquired (or WithValue option is used).
func (m *ReadMutex) Value() string {
	return m.value
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *ReadMutex) Until() time.Time {
	return m.until
}

// TryLock only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *ReadMutex) TryLock() error {
	return m.TryLockContext(context.Background())
}

// TryLockContext only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *ReadMutex) TryLockContext(ctx context.Context) error {
	return m.lockContext(ctx, 1)
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *ReadMutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *ReadMutex) LockContext(ctx context.Context) error {
	return m.lockContext(ctx, m.tries)
}

func (m *ReadMutex) lockContext(ctx context.Context, tries int) (err error) {
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
			//TODO: fetch TTL
			m.value = value
			m.until = until
			return nil
		} else if result {
			_, err = m.Unlock()
			if err != nil {
				log.Printf("unlock when timeout failed \n")
			}
		}

		if i == m.tries-1 && err != nil {
			return err
		}
	}

	return ErrLockFailed
}
func (m *ReadMutex) Unlock() (bool, error) {
	return m.UnlockContext(context.Background())
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *ReadMutex) UnlockContext(ctx context.Context) (bool, error) {
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
			log.Printf("del unlock latch failed, count: %d, err: %s", cnt, err.Error())
		}
		return result, err
	}

	return true, nil
}

// KEY: KEY, {KEY}:{SERVICE_ID:THREAD_ID}:rwlock_timeout,
// ARGV: EXPIRY, SERVICE_ID:THREAD_ID, SERVICE_ID:THREAD_ID:write
// redis structure:
//
//		[MAP]-KEY: {
//		  "mode": "read" | "write",
//		  [SERVICE_ID:THREAD_ID]: 1...n,
//		}
//
//	 [STRING]-{KEY}:{SERVICE_ID:THREAD_ID}:rwlock_timeout:1  1
var acquireReadScript = `
local mode = redis.call('hget', KEYS[1], 'mode'); 
if (mode == false) then 
redis.call('hset', KEYS[1], 'mode', 'read'); 
redis.call('hset', KEYS[1], ARGV[2], 1); 
redis.call('set', KEYS[2] .. ':1', 1); 
redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); 
redis.call('pexpire', KEYS[1], ARGV[1]); 
return nil; 
end; 
if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then 
local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); 
local key = KEYS[2] .. ':' .. ind;
redis.call('set', key, 1); 
redis.call('pexpire', key, ARGV[1]); 
local remainTime = redis.call('pttl', KEYS[1]); 
redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); 
return nil; 
end;
return redis.call('pttl', KEYS[1]);
`

func (m *ReadMutex) acquire(ctx context.Context, value string) (bool, error) {
	cmd := m.client.Eval(ctx, acquireReadScript, []string{m.name,
		m.name + ":" + value + ":rwlock_timeout"},
		m.expiry/time.Millisecond, value, value+":write")
	// how to check script's return value is nil?
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

// KEY: KEY, redisson_rwlock:{KEY}, {KEY}:{VALUE}:rwlock_timeout, {KEY}, redisson_unlock_latch:{KEY}:{requestId}
// ARGS: 0,value, SPUBLISH/PUBLISH, timeout
var releaseReadScript = `
local val = redis.call('get', KEYS[5]); 
if val ~= false then 
return tonumber(val);
end; 
local mode = redis.call('hget', KEYS[1], 'mode'); 
if (mode == false) then 
redis.call(ARGV[3], KEYS[2], ARGV[1]); 
redis.call('set', KEYS[5], 1, 'px', ARGV[4]); 
return 1; 
end; 
local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); 
if (lockExists == 0) then 
return nil;
end; 
local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); 
if (counter == 0) then 
redis.call('hdel', KEYS[1], ARGV[2]); 
end;
redis.call('del', KEYS[3] .. ':' .. (counter+1)); 
if (redis.call('hlen', KEYS[1]) > 1) then 
local maxRemainTime = -3; 
local keys = redis.call('hkeys', KEYS[1]); 
for n, key in ipairs(keys) do 
counter = tonumber(redis.call('hget', KEYS[1], key)); 
if type(counter) == 'number' then 
for i=counter, 1, -1 do 
local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); 
maxRemainTime = math.max(remainTime, maxRemainTime);
end; 
end; 
end; 
if maxRemainTime > 0 then 
redis.call('pexpire', KEYS[1], maxRemainTime); 
redis.call('set', KEYS[5], 0, 'px', ARGV[4]); 
return 0; 
end;
if mode == 'write' then 
redis.call('set', KEYS[5], 0, 'px', ARGV[4]); 
return 0;
end; 
end; 
redis.call('del', KEYS[1]); 
redis.call(ARGV[3], KEYS[2], ARGV[1]); 
redis.call('set', KEYS[5], 1, 'px', ARGV[4]); 
return 1; 
`

func (m *ReadMutex) release(ctx context.Context, value string, requestId string) (bool, error) {
	cmd := m.client.Eval(ctx, releaseReadScript, []string{m.name,
		m.getChannelName(m.name),
		m.getTimeoutKeyName(m.name, value),
		m.name,
		m.getUnlockLatchName(m.name, value, requestId),
	}, 0, value, "PUBLISH", m.expiry/time.Millisecond)
	result, err := cmd.Result()
	if err != nil {
		return false, err
	}

	if result == nil {
		return true, nil
	}

	i, err := cmd.Int()
	if err != nil {
		return false, err
	}

	if i >= 1 {
		return false, nil
	}

	return true, nil
}

func (m *ReadMutex) getChannelName(key string) string {
	return "redisson_rwlock:" + key
}

func (m *ReadMutex) getTimeoutKeyName(key, value string) string {
	return key + ":" + value + ":rwlock_timeout"
}

func (m *ReadMutex) getUnlockLatchName(key, value, requestId string) string {
	return "redisson_unlock_latch:" + key + ":" + value + ":" + requestId
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

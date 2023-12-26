package mutex

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/jonahcui/go-redisson/pubsub"
	"log"
	"time"
)

type FairLock struct {
	name string

	internalLockLeaseTime time.Duration

	value        string
	client       *redis.Client
	pubsub       *pubsub.LockPubSub
	watchDogFlag chan int
}

func (f *FairLock) TryLock(parentCtx context.Context, waitTime time.Duration, lease time.Duration) error {
	ctx, cancel := context.WithTimeout(parentCtx, waitTime)
	defer cancel()

	value, err := genValue()
	if err != nil {
		return err
	}

	start := time.Now().UnixMilli()
	ttl, err := f.acquireFairLockWithLongReturn(ctx, value, lease, waitTime)
	end := time.Now().UnixMilli()

	if err != nil {
		return err
	}

	// get lock
	if ttl == nil && err == nil {
		log.Printf("get lock %s from %s successful", f.name, f.value)
		f.value = value
		if lease.Milliseconds() > end-start {
			f.internalLockLeaseTime = time.Duration(lease.Milliseconds()-(end-start)) * time.Millisecond
		} else {
			//TODO: do guard dog logic
		}
		return err
	}

	// wait notifier
	msg := make(chan int)

	err = f.pubsub.Subscribe(ctx, f.channelName()+":"+value, value, msg)
	if err != nil {
		return err
	}

	timer := time.NewTimer(time.Duration(*ttl) * time.Millisecond)
	unsubscribe := func() {
		timer.Stop()
		f.pubsub.Unsubscribe(f.channelName(), value)
	}
	for {
		select {
		case <-ctx.Done():
			unsubscribe()
			return ErrTimeout
		case <-timer.C:
			{
				unsubscribe()
				return ErrTimeout
			}
		case <-msg:
			{
				end = time.Now().UnixMilli()
				timeoutCtx, cancel := context.WithTimeout(ctx, waitTime-time.Duration(end-start)*time.Millisecond)
				nttl, err := f.acquireFairLockWithLongReturn(timeoutCtx, value, waitTime, lease)
				cancel()

				if err != nil {
					unsubscribe()
					return err
				}

				if nttl == nil {
					f.value = value
					unsubscribe()
					return nil
				}
				// because I use timeout to control the channel, so I don't need to renew the ttl for the pubsub.
				// in the java version redisson, the listener was renewed
			}
		}
	}
}

func (f *FairLock) Unlock(ctx context.Context, requestId string, retries int, latchTimeout time.Duration) (bool, error) {
	requestId, err := genValue()
	if err != nil {
		return false, err
	}
	for i := 0; i < retries; i++ {
		result, err := f.release(ctx, f.value, requestId, latchTimeout)
		log.Printf("unlock value %s from %s,  result: %t, err: %s", f.value, requestId, result, err)
		if err != nil {
			continue
		}

		cnt, err := f.client.Del(ctx, f.unlockLatchName(requestId)).Result()
		if err != nil || cnt < 1 {
			log.Printf("del unlock latch failed, count: %d, err: %s", cnt, err)
		}
		return result, err
	}

	return true, nil
}

var (
	acquireFairLock = `
while true do 
local firstThreadId2 = redis.call('lindex', KEYS[2], 0);
if firstThreadId2 == false then 
break;
end;
local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));
if timeout <= tonumber(ARGV[3]) then 
redis.call('zrem', KEYS[3], firstThreadId2);
redis.call('lpop', KEYS[2]);
else 
break;
end;
end;
if (redis.call('exists', KEYS[1]) == 0) 
and ((redis.call('exists', KEYS[2]) == 0) 
or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then 
redis.call('lpop', KEYS[2]);
redis.call('zrem', KEYS[3], ARGV[2]);
local keys = redis.call('zrange', KEYS[3], 0, -1);
for i = 1, #keys, 1 do 
redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);
end;
redis.call('hset', KEYS[1], ARGV[2], 1);
redis.call('pexpire', KEYS[1], ARGV[1]);
return nil;
end;
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then 
redis.call('hincrby', KEYS[1], ARGV[2], 1);
redis.call('pexpire', KEYS[1], ARGV[1]);
return nil;
end;
return 1;
`
)

func (f *FairLock) acquire(ctx context.Context, value string, leaseTime time.Duration, waitTime time.Duration) (*int, error) {
	cmd := f.client.Eval(ctx, acquireFairLock, []string{f.name, f.threadQueueName(),
		f.timeoutSetName()}, leaseTime.Milliseconds(), value, time.Now().UnixMilli(), waitTime.Milliseconds())
	if errors.Is(cmd.Err(), redis.Nil) {
		return nil, nil
	}
	i, err := cmd.Int()
	return &i, err
}

var acquireFairLockWithLongReturn = `
while true do 
local firstThreadId2 = redis.call('lindex', KEYS[2], 0);
if firstThreadId2 == false then 
break;
end;
local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));
if timeout <= tonumber(ARGV[4]) then 
redis.call('zrem', KEYS[3], firstThreadId2);
redis.call('lpop', KEYS[2]);
else 
break;
end;
end;
if (redis.call('exists', KEYS[1]) == 0) 
and ((redis.call('exists', KEYS[2]) == 0) 
or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then 
redis.call('lpop', KEYS[2]);
redis.call('zrem', KEYS[3], ARGV[2]);
local keys = redis.call('zrange', KEYS[3], 0, -1);
for i = 1, #keys, 1 do 
redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);
end;
redis.call('hset', KEYS[1], ARGV[2], 1);
redis.call('pexpire', KEYS[1], ARGV[1]);
return nil;
end;
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then 
redis.call('hincrby', KEYS[1], ARGV[2],1);
redis.call('pexpire', KEYS[1], ARGV[1]);
return nil;
end;
local timeout = redis.call('zscore', KEYS[3], ARGV[2]);
if timeout ~= false then 
return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);
end;
local lastThreadId = redis.call('lindex', KEYS[2], -1);
local ttl;
if lastThreadId ~= false and lastThreadId ~= ARGV[2] then 
ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);
else 
ttl = redis.call('pttl', KEYS[1]);
end;
local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);
if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then 
redis.call('rpush', KEYS[2], ARGV[2]);
end;
return ttl;
`

func (f *FairLock) acquireFairLockWithLongReturn(ctx context.Context, value string, leaseTime time.Duration, waitTime time.Duration) (*int, error) {
	cmd := f.client.Eval(ctx, acquireFairLockWithLongReturn, []string{f.name, f.threadQueueName(),
		f.timeoutSetName()}, leaseTime.Milliseconds(), value, waitTime.Milliseconds(), time.Now().UnixMilli())
	if errors.Is(cmd.Err(), redis.Nil) {
		return nil, nil
	}

	ttl, err := cmd.Int()
	return &ttl, err
}

var fairLockReleaseScript = `
local val = redis.call('get', KEYS[5]); 
if val ~= false then 
return tonumber(val);
end; 
while true do 
local firstThreadId2 = redis.call('lindex', KEYS[2], 0);
if firstThreadId2 == false then 
break;
end; 
local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));
if timeout <= tonumber(ARGV[4]) then 
redis.call('zrem', KEYS[3], firstThreadId2); 
redis.call('lpop', KEYS[2]); 
else 
break;
end; 
end;
if (redis.call('exists', KEYS[1]) == 0) then 
local nextThreadId = redis.call('lindex', KEYS[2], 0); 
if nextThreadId ~= false then 
redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1]); 
end; 
redis.call('set', KEYS[5], 1, 'px', ARGV[6]); 
return 1; 
end;
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then 
return nil;
end; 
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); 
if (counter > 0) then 
redis.call('pexpire', KEYS[1], ARGV[2]); 
redis.call('set', KEYS[5], 0, 'px', ARGV[6]); 
return 0; 
end; 
redis.call('del', KEYS[1]); 
redis.call('set', KEYS[5], 1, 'px', ARGV[6]); 
local nextThreadId = redis.call('lindex', KEYS[2], 0); 
if nextThreadId ~= false then 
redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1]); 
end; 
return 1; 
`

func (f *FairLock) release(ctx context.Context, value, requestId string, latchTimeout time.Duration) (bool, error) {
	cmd := f.client.Eval(ctx, fairLockReleaseScript, []string{
		f.name, f.threadQueueName(), f.timeoutSetName(), f.channelName(), f.unlockLatchName(requestId),
	}, UnlockMessage, f.internalLockLeaseTime, value, time.Now().UnixMilli(), "PUBLISH", latchTimeout.Milliseconds())

	if errors.Is(cmd.Err(), redis.Nil) {
		return false, nil
	}

	count, err := cmd.Int()
	if err != nil {
		return false, err
	}

	if count == 0 {
		return true, err
	}
	if count == 1 {
		return false, err
	}

	return true, err
}

func (f *FairLock) threadQueueName() string {
	return "redisson_lock_queue:" + f.name
}

func (f *FairLock) timeoutSetName() string {
	return "redisson_lock_timeout:" + f.name
}

func (f *FairLock) channelName() string {
	return "redisson_lock__channel:" + f.name
}

func (f *FairLock) unlockLatchName(requestId string) string {
	return "redisson_unlock_latch:" + f.name + ":" + requestId
}

package mutex

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

type Lock struct {
	name      string
	leaseTime uint

	internalLockLeaseTime uint
	client                *redis.Client
}

var (
	lockAcquireScript = `
if ((redis.call('exists', KEYS[1]) == 0) 
or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then 
redis.call('hincrby', KEYS[1], ARGV[2], 1); 
redis.call('pexpire', KEYS[1], ARGV[1]); 
return nil; 
end; 
return redis.call('pttl', KEYS[1]);
`
)

func (l *Lock) acquire(ctx context.Context, value string) (bool, error) {
	cmd := l.client.Eval(ctx, lockAcquireScript, []string{l.name}, l.leaseTime, value)
	if errors.Is(cmd.Err(), redis.Nil) {
		return true, nil
	}
	if cmd.Err() != nil {
		return false, cmd.Err()
	}

	return false, nil
}

var (
	lockReleaseScript = `
local val = redis.call('get', KEYS[3]); 
if val ~= false then 
return tonumber(val);
end; 
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then 
return nil;
end; 
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); 
if (counter > 0) then 
redis.call('pexpire', KEYS[1], ARGV[2]); 
redis.call('set', KEYS[3], 0, 'px', ARGV[5]); 
return 0; 
else 
redis.call('del', KEYS[1]); 
redis.call(ARGV[4], KEYS[2], ARGV[1]); 
redis.call('set', KEYS[3], 1, 'px', ARGV[5]); 
return 1; 
end; 
`
)

func (l *Lock) release(ctx context.Context, value string) error {
	cmd := l.client.Eval(ctx, lockReleaseScript, []string{l.name, l.channelName(), l.unlockLatchName(value)},
		UnlockMessage, l.internalLockLeaseTime, value, "publish", l.leaseTime)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

func (l *Lock) channelName() string {
	return "redisson_lock__channel:" + l.name
}

func (l *Lock) unlockLatchName(value string) string {
	return "redisson_unlock_latch:" + l.name + ":" + value
}

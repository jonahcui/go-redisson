package mutex

import (
	"context"
	"github.com/jonahcui/go-redisson/pubsub"
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestFairLockAcquireAndRelease(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockPubSub := pubsub.NewLockPubSub(client)

	f := &FairLock{
		name:                  "test_fair_lock",
		internalLockLeaseTime: 100 * time.Second,
		pubsub:                lockPubSub,
		client:                client,
	}

	log.Printf("start get lock1")
	ctx := context.Background()
	err := f.TryLock(ctx, 5*time.Second, 100*time.Second)
	assert.NoError(t, err)
	log.Printf("get lock1 successful")
	time.AfterFunc(3*time.Second, func() {
		log.Printf("start unlock lock1")
		_, err := f.Unlock(context.Background(), "requestId", 3, 5*time.Second)
		assert.NoError(t, err)
	})

	f2 := &FairLock{
		name:                  "test_fair_lock",
		internalLockLeaseTime: 100 * time.Second,
		pubsub:                lockPubSub,
		client:                client,
	}
	log.Printf("start get lock2")
	err = f2.TryLock(context.Background(), 10*time.Second, 100*time.Second)
	assert.NoError(t, err)

	_, err = f2.Unlock(ctx, "requestId2", 3, 5*time.Second)
	assert.NoError(t, err)
}

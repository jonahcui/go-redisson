package pubsub

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
	"time"
)

var (
	UnlockNotifier = 1
	ChannelClosed  = 2
)

type RedissonLockEntry struct {
	sync.Mutex
	queue     []chan int
	entryName []string
	end       chan int
	waitTime  sync.Map
}

func (r *RedissonLockEntry) AddToQueue(entryName string, item chan int, waitTime int64) {
	r.Lock()
	defer r.Unlock()
	r.queue = append(r.queue, item)
	r.entryName = append(r.entryName, entryName)
	r.waitTime.Store(entryName, waitTime)
}

// Remove a listener for the special entry
// if the entryName is not exist, do nothing
// if the queue has only one item, close the channel
func (r *RedissonLockEntry) Remove(entryName string) (shouldClose bool) {
	r.Lock()
	defer r.Unlock()
	for i, name := range r.entryName {
		if name == entryName {
			r.entryName = append(r.entryName[:i], r.entryName[i+1:]...)
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			r.waitTime.Delete(entryName)
			break
		}
	}

	if len(r.queue) == 0 {
		r.close()
	}
	return len(r.queue) == 0
}

func (r *RedissonLockEntry) close() {
	r.end <- 1
}

func (r *RedissonLockEntry) watchChannel(pubsub *redis.PubSub) {
	messages := pubsub.ChannelWithSubscriptions(context.Background(), 30)
	for {
		select {
		case msg := <-messages:
			{
				switch msg.(type) {
				case *redis.Subscription:
					{
						log.Printf("subscribed to channel %s \n", msg.(*redis.Subscription).Channel)
					}
				case *redis.Message:
					{
						m := msg.(*redis.Message)
						log.Printf("received message %s from channel %s \n", m.Payload, m.Channel)
						if m.Payload == "0" {
							r.Lock()
							for len(r.queue) > 0 {
								firstNode := r.entryName[0]
								if waitTime, ok := r.waitTime.Load(firstNode); ok && waitTime.(int64) > time.Now().UnixMilli() {
									r.queue[0] <- UnlockNotifier
									r.queue = r.queue[1:]
									r.entryName = r.entryName[1:]
									break
								}
								r.queue = r.queue[1:]
								r.entryName = r.entryName[1:]
							}
							r.Unlock()
						} else if m.Payload == "1" {
							//READ UNLOCK MSG
						}
					}
				}

			}
		case <-r.end:
			{
				if err := pubsub.Close(); err != nil {
					// panic or log
				}
				return
			}
		}
	}
}

type LockPubSub struct {
	client  *redis.Client
	entries map[string]*RedissonLockEntry
	sync.Mutex
}

func NewLockPubSub(client *redis.Client) *LockPubSub {
	return &LockPubSub{
		client:  client,
		entries: make(map[string]*RedissonLockEntry),
	}
}

func (ps *LockPubSub) Subscribe(ctx context.Context, channel string, entryName string, queue chan int, waitTime int64) error {
	// if the ctx has done, return immediately
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if _, ok := ps.entries[channel]; !ok {
		ps.Lock()
		if _, ok := ps.entries[channel]; !ok {
			entry := &RedissonLockEntry{
				queue: make([]chan int, 0),
				end:   make(chan int),
			}

			ps.entries[channel] = entry
			log.Printf("start watch channel %s for %s \n", channel, entryName)
			subscribe := ps.client.Subscribe(ctx, channel)
			go entry.watchChannel(subscribe)
		}
		ps.Unlock()
	}
	entry := ps.entries[channel]

	entry.AddToQueue(entryName, queue, waitTime)

	// fix this
	go func() {
		<-ctx.Done()
		ps.Unsubscribe(channel, entryName)
	}()

	return nil
}

func (ps *LockPubSub) Unsubscribe(channel string, entryName string) {
	if _, ok := ps.entries[channel]; !ok {
		return
	}
	entry := ps.entries[channel]
	if entry.Remove(entryName) {
		ps.Lock()
		delete(ps.entries, channel)
		ps.Unlock()
	}
}

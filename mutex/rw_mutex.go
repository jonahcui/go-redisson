package mutex

import (
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	ErrRecordNotFound = ""
)

type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type RWMutex struct {
	name   string
	expiry time.Duration

	tries         int
	retryInterval time.Duration
	timeout       time.Duration

	genValueFunc func() (string, error)
	value        string
	until        time.Time
	client       *redis.Client

	readLock  *ReadMutex
	writeLock *WriteMutex
}

type RwMutexOptionFunc func(*RWMutex)

func WithTries(tries int) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.tries = tries
	}
}

func WithRetryInterval(retryInterval time.Duration) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.retryInterval = retryInterval
	}
}

func WithTimeout(timeout time.Duration) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.timeout = timeout
	}
}

func WithClient(client *redis.Client) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.client = client
	}
}

func WithGenValueFunc(genValueFunc func() (string, error)) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.genValueFunc = genValueFunc
	}
}

func WithName(name string) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.name = name
	}
}

func WithExpiry(expiry time.Duration) RwMutexOptionFunc {
	return func(rw *RWMutex) {
		rw.expiry = expiry
	}
}

func NewRWMutex(options ...RwMutexOptionFunc) *RWMutex {
	rw := &RWMutex{
		genValueFunc: genValue,
	}
	for _, option := range options {
		option(rw)
	}

	return rw
}

func (rw *RWMutex) ReadLock() *ReadMutex {
	if rw.readLock == nil {
		rw.readLock = &ReadMutex{
			name:   rw.name,
			expiry: rw.expiry,

			tries:         rw.tries,
			retryInterval: rw.retryInterval,
			timeout:       rw.timeout,

			genValueFunc: func() (string, error) {
				if rw.Value() == "" {
					return rw.genValueFunc()
				} else {
					return rw.Value(), nil
				}
			},
			value:  rw.value,
			until:  rw.until,
			client: rw.client,
		}
	}
	return rw.readLock
}

func (rw *RWMutex) WriteLock() *WriteMutex {
	if rw.writeLock == nil {
		rw.writeLock = &WriteMutex{
			name:   rw.name,
			expiry: rw.expiry,

			tries:         rw.tries,
			retryInterval: rw.retryInterval,
			timeout:       rw.timeout,

			genValueFunc: func() (string, error) {
				if rw.Value() == "" {
					return rw.genValueFunc()
				} else {
					return rw.Value(), nil
				}
			},

			value:  rw.value,
			until:  rw.until,
			client: rw.client,
		}
	}
	return rw.writeLock
}

func (rw *RWMutex) Value() string {
	if rw.readLock != nil && rw.readLock.value != "" {
		return rw.readLock.value
	}
	if rw.writeLock != nil && rw.writeLock.value != "" {
		return rw.writeLock.value
	}
	return ""
}

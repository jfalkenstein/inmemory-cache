package inmemorycache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MauriceGit/skiplist"
)

const (
	DefaultTimeBetweenExpirations = 1 * time.Minute
	DefaultTTL                    = 24 * time.Hour
	DefaultBufferSize = 1000
)

// Cacher is a simple component for an in-memory cache. All values stored and retrieved
// are []byte, making this useful for handling any type of data.
type Cacher interface {
	// Get retrieves a cached value, if it exists
	Get(key string) (value []byte, isHit bool)
	// Set sets a value onto the cache for a given key. If no ttlOrExpiry is passed,
	// it will use the default TTL for keys set. Otherwise, a time.Duration TTL or a
	// or time.Time expiry can be passed to explicitly set the cache expiration time.
	// This can be called successive times for a given key to update the value and/or
	// expiration on a key.
	Set(key string, value []byte, ttlOrExpiry ...any)
	// Delete removes a key from the cache.
	Delete(key string)
	// Close terminates the cache. Call this at the conclusion of operation.
	Close()
}

// CacheNode is a small data structure used to record and track a cached value.
type CacheNode struct {
	key        string
	value      []byte
	expiration int64
}

// ExtractKey implements the skiplist.ListElement interface, used as the "score" for
// keeping expirations sorted
func (c *CacheNode) ExtractKey() float64 {
	return float64(c.expiration)
}

func (c *CacheNode) String() string {
	return c.key
}

// CacherOptions are specific configurations used for the cache struct
type CacherOptions struct {
	// DefaultTTL is the default time-to-live used for cached values that are
	// set without specifying a ttlOrExpiry. Cached values will not be retrievable
	// past this time period.
	DefaultTTL                  time.Duration
	// TimeBetweenExpirationChecks is the interval between cache-clearing cycles where
	// expired values will be removed from the cache.
	TimeBetweenExpirationChecks time.Duration
	// NowFunc is a function used to obtain the current time. This will default
	// to time.Now().UTC() and shouldn't need to be updated in production. However,
	// This is quite useful for overriding the current time in tests.
	NowFunc func() time.Time
}

// cache is a simple, thread-safe implementation of the Cacher interface. It runs a 
// periodic, background goroutine that will clear out expired cache items. Even if that
// process hasn't run yet, though, the cache guarantees that no expired items will be 
// retrieved by the user.
type cache struct {
	innerCache  sync.Map
	expirations skiplist.SkipList
	now         func() time.Time

	defaultTTL                  time.Duration
	timeBetweenExpirationChecks time.Duration
	newExpirationsChannel       chan *CacheNode
	deletionsChannel            chan *CacheNode
	stopChannel                 chan bool
	isStopped                   atomic.Bool
}

// NewCacher is the constructor for the default implementation of Cacher. It will
// be configured with a default set of options. Pass an options function to alter
// those defaults as desired.
func NewCacher(opts ...func(*CacherOptions)) Cacher {
	options := CacherOptions{
		NowFunc:                     func() time.Time { return time.Now().UTC() },
		TimeBetweenExpirationChecks: DefaultTimeBetweenExpirations,
		DefaultTTL: DefaultTTL,
	}
	for _, opt := range opts {
		opt(&options)
	}

	c := cache{
		innerCache:                  sync.Map{},
		expirations:                 skiplist.New(),
		now:                         options.NowFunc,
		timeBetweenExpirationChecks: options.TimeBetweenExpirationChecks,

		newExpirationsChannel: make(chan *CacheNode, 1000),
		deletionsChannel:      make(chan *CacheNode, 1000),
		stopChannel:           make(chan bool, 1),
	}
	c.startExpirationCycle()
	return &c
}

func (c *cache) startExpirationCycle() {
	go func() {
		ticker := time.NewTicker(c.timeBetweenExpirationChecks)
		for {
			select {
			case node, ok := <-c.newExpirationsChannel:
				if !ok {
					continue
				}
				c.expirations.Insert(node)
			case node, ok := <-c.deletionsChannel:
				if !ok {
					continue
				}
				c.expirations.Delete(node)
			case <-ticker.C:
				c.expire()
			case <-c.stopChannel:
				return
			}
		}
	}()
}

func (c *cache) Close() {
	if c.isStopped.Load() {
		return
	}
	c.stopChannel <- true
	close(c.newExpirationsChannel)
	close(c.deletionsChannel)
	c.isStopped.Store(true)
}

func (c *cache) Get(key string) ([]byte, bool) {
	if c.isStopped.Load() {
		panic("cache has already been closed")
	}
	node, ok := c.innerCache.Load(key)
	if !ok {
		return nil, false
	}
	asNode := node.(*CacheNode)
	now := c.now().UnixMilli()
	if now > asNode.expiration {
		return nil, false
	}
	return asNode.value, true
}

func (c *cache) Set(key string, value []byte, ttlOrExpiry ...any) {
	if c.isStopped.Load() {
		panic("cache has already been closed")
	}
	expiration := c.parseTtlOrExpiry(ttlOrExpiry)
	asNode := CacheNode{key: key, value: value, expiration: expiration.UTC().UnixMilli()}
	// We want to use LoadOrStore here because we need to know if the key was already
	// set. If it was, we want to replace it in the expirations list; otherwise, it'll
	// be in the wrong order in the skip-list.
	if existing, ok := c.innerCache.LoadOrStore(key, &asNode); ok {
		c.deletionsChannel <- &asNode
		existingNode := existing.(*CacheNode)
		existingNode.value = value
		existingNode.expiration = asNode.expiration
		c.newExpirationsChannel <- existingNode
	} else {
		c.newExpirationsChannel <- &asNode
	}
}

func (c *cache) parseTtlOrExpiry(ttlOrExpiry []any) time.Time {
	var toParse any
	if len(ttlOrExpiry) == 0 {
		toParse = c.defaultTTL
	} else {
		toParse = ttlOrExpiry[0]
	}

	switch val := toParse.(type) {
	case time.Time:
		return val
	case time.Duration:
		return c.now().Add(val)
	default:
		panic(fmt.Sprintf("unexpected type for ttlOrExpiry: %T", toParse))
	}
}

func (c *cache) Delete(key string) {
	if c.isStopped.Load() {
		panic("cache has already been closed")
	}
	if value, ok := c.innerCache.LoadAndDelete(key); ok {
		c.deletionsChannel <- value.(*CacheNode)
	}
}

func (c *cache) delete(node *CacheNode) {
	c.innerCache.Delete(node.key)
	c.expirations.Delete(node)
}

func (c *cache) expire() {
	now := c.now()
	nowTimestamp := now.UnixMilli()
	element := c.expirations.GetSmallestNode()
	toRemove := make([]*CacheNode, 0)

	for {
		if element == nil {
			return
		}
		node := element.GetValue().(*CacheNode)
		if node.expiration < nowTimestamp {
			toRemove = append(toRemove, node)
			element = c.expirations.Next(element)
		} else {
			break
		}
	}
	for _, node := range toRemove {
		c.delete(node)
	}
}

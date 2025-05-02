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
	DefaultTTL = 24 * time.Hour
)

type Cacher interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, ttlOrExpiration ...any)
	Delete(key string)
	Close()
}

type CacheNode struct {
	key        string
	value      []byte
	expiration int64
}

func (c *CacheNode) ExtractKey() float64 {
	return float64(c.expiration)
}

func (c *CacheNode) String() string {
	return c.key
}

type CacherOptions struct {
	NowFunc                     func() time.Time
	TimeBetweenExpirationChecks time.Duration
	DefaultTTL time.Duration
}

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

func NewCacher(opts ...func(*CacherOptions)) Cacher {
	options := CacherOptions{
		NowFunc:                     func() time.Time { return time.Now().UTC() },
		TimeBetweenExpirationChecks: DefaultTimeBetweenExpirations,
	}
	for _, opt := range opts {
		opt(&options)
	}

	c := cache{
		innerCache:                  sync.Map{},
		expirations:                 skiplist.New(),
		now:                         options.NowFunc,
		timeBetweenExpirationChecks: options.TimeBetweenExpirationChecks,

		newExpirationsChannel: make(chan *CacheNode, 100),
		deletionsChannel:      make(chan *CacheNode, 100),
		stopChannel:           make(chan bool, 1),
	}
	c.start()
	return &c
}

func (c *cache) start() {
	ticker := time.NewTicker(c.timeBetweenExpirationChecks)
	go func() {
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
	if element == nil {
		return
	}
	toRemove := make([]*CacheNode, 0)

	for {
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

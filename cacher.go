package inmemorycache

import (
	"sync"
	"time"
	"sync/atomic"

	"github.com/MauriceGit/skiplist"
)

const DefaultTimeBetweenExpirations = 1 * time.Minute

type Cacher interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, expiration time.Time)
	Delete(key string)
}

type CacheNode struct {
	key string
	value []byte
	expiration int64
}

func (c *CacheNode) ExtractKey() float64 {
	return float64(c.expiration)
}

func (c *CacheNode) String() string {
	return c.key
}


type cache struct {
	innerCache sync.Map
	expirations skiplist.SkipList
	lock sync.RWMutex
	now func() time.Time

	timeBetweenExpirations time.Duration
	lastExpirationRun atomic.Int64
}

func NewCacher(timeBetweenExpirations time.Duration) Cacher {
	c := cache{
		innerCache: sync.Map{},
		expirations: skiplist.New(),
		now: func() time.Time {return time.Now().UTC()},
		timeBetweenExpirations: timeBetweenExpirations,
	}
	c.lastExpirationRun.Store(time.Now().UTC().UnixMilli())
	return &c
}

func (c *cache) Get(key string) ([]byte, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	node, ok := c.innerCache.Load(key)
	if !ok {
		return nil, false
	}
	asNode := node.(*CacheNode)
	now := c.now().UnixMilli()
	if now > asNode.expiration {
		return nil, false
	}
	go c.expire()
	return asNode.value, true
}

func (c *cache) Set(key string, value []byte, expiration time.Time) {
	c.lock.Lock()
	defer c.lock.Unlock()
	asNode := CacheNode{ key: key, value: value, expiration: expiration.UTC().UnixMilli()}
	if existing, ok := c.innerCache.LoadOrStore(key, &asNode); ok {
		existing.(*CacheNode).value = value
		existing.(*CacheNode).expiration = asNode.expiration
	} else {
		c.expirations.Insert(&asNode)
	}
	go c.expire()
}

func (c *cache) Delete(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if value, ok := c.innerCache.Load(key); ok {
		c.delete(value.(*CacheNode))
	}
	go c.expire()
}

func (c *cache) delete(node *CacheNode){
	c.innerCache.Delete(node.key)
	c.expirations.Delete(node)
}

func (c *cache) expire(){
	lastExpireTimeStamp := c.lastExpirationRun.Load()
	lastExpireTime := time.UnixMilli(lastExpireTimeStamp)
	now := c.now()

	if !lastExpireTime.Add(c.timeBetweenExpirations).Before(now) {
		return
	}
	nowTimestamp := now.UnixMilli()
	if c.lastExpirationRun.CompareAndSwap(lastExpireTimeStamp, nowTimestamp){
		c.lock.Lock()
		defer c.lock.Unlock()
		element := c.expirations.GetSmallestNode()
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
		for _, node := range toRemove{
			c.delete(node)
		}
	}
}


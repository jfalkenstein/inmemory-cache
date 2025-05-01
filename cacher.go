package inmemorycache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/MauriceGit/skiplist"
)

const DefaultTimeBetweenExpirations = 1 * time.Minute

type Cacher interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, expiration time.Time)
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

type cache struct {
	innerCache  sync.Map
	expirations skiplist.SkipList
	now         func() time.Time

	timeBetweenExpirations time.Duration

	newExpirationsChannel chan *CacheNode
	updationsChannel      chan *CacheNode
	deletionsChannel      chan *CacheNode
	stopChannel           chan bool
	isStopped atomic.Bool
}

func (c *cache) start() {
	ticker := time.NewTicker(c.timeBetweenExpirations)
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
			case <- c.stopChannel:
				c.isStopped.Store(true)
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
}

func NewCacher(timeBetweenExpirations time.Duration) Cacher {
	c := cache{
		innerCache:             sync.Map{},
		expirations:            skiplist.New(),
		now:                    func() time.Time { return time.Now().UTC() },
		timeBetweenExpirations: timeBetweenExpirations,
		newExpirationsChannel:  make(chan *CacheNode, 100),
		updationsChannel:       make(chan *CacheNode, 100),
		deletionsChannel:       make(chan *CacheNode, 100),
		stopChannel:            make(chan bool, 1),
	}
	c.start()
	return &c
}

func (c *cache) Get(key string) ([]byte, bool) {
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

func (c *cache) Set(key string, value []byte, expiration time.Time) {
	asNode := CacheNode{key: key, value: value, expiration: expiration.UTC().UnixMilli()}
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

func (c *cache) Delete(key string) {
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

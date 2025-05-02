package inmemorycache

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

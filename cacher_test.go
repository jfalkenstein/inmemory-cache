package inmemorycache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cache := NewCacher(1 * time.Millisecond)
	wg := sync.WaitGroup{}
	for i := range 10000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			now := time.Now()
			key := fmt.Sprintf("key %d", i)
			value := fmt.Sprintf("value %d", i)
			cache.Set(
				key,
				[]byte(value),
				now.Add(10*time.Millisecond),
			)
			result, ok := cache.Get(key)
			if !ok {
				t.Log("key not available")
				t.Fail()
			} else if string(result) != value {
				t.Logf("Unexpected value: %v", result)
				t.Fail()
			} else {
				t.Logf("It worked %d", i)
			}
			time.Sleep(11 * time.Millisecond)
			_, ok = cache.Get(key)
			if ok {
				t.Logf("Got a record when I shouldn't have!")
				t.Fail()
			}
		}(i)
	}
	wg.Wait()
	cache.Close()
}

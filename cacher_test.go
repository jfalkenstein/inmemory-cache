package inmemorycache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cache := NewCacher(1 * time.Minute)
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
				now.Add(1*time.Minute),
			)
			result, ok := cache.Get(key)
			if !ok {
				t.Log("")
				t.Fail()
			} else if string(result) != value {
				t.Logf("Unexpected value: %v", result)
				t.Fail()
			} else {
				t.Logf("It worked %d", i)
			}
		}(i)
	}
	wg.Wait()
}

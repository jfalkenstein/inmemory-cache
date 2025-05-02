package inmemorycache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cache := NewCacher(1 * time.Millisecond)
	defer cache.Close()
	wg := sync.WaitGroup{}

	for i := range 10001 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			now := time.Now()
			key := fmt.Sprintf("key %d", i)
			value := fmt.Sprintf("value %d", i)
			randomMsecs := rand.Intn(100)
			randomMsecs += 20
			cache.Set(
				key,
				[]byte(value),
				now.Add(time.Duration(randomMsecs)*time.Millisecond),
			)
			result, ok := cache.Get(key)
			if !ok {
				t.Logf("key not available: %s", key)
				t.Fail()
			} else if string(result) != value {
				t.Logf("Unexpected value: %v", result)
				t.Fail()
			} else {
				t.Logf("It worked %d", i)
			}
			time.Sleep(time.Duration(randomMsecs+1) * time.Millisecond)
			_, ok = cache.Get(key)
			if ok {
				t.Logf("Got a record when I shouldn't have!")
				t.Fail()
			}
		}(i)
	}
	wg.Wait()
}

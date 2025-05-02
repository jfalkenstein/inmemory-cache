package inmemorycache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CacherSuite struct {
	suite.Suite

	now                    time.Time
	timeBetweenExpirations time.Duration

	cacher Cacher
}

func (c *CacherSuite) SetupTest() {
	c.now = time.Date(2020, 2, 2, 2, 2, 2, 0, time.UTC)
	c.timeBetweenExpirations = 1 * time.Millisecond

	c.cacher = NewCacher(func(co *CacherOptions) {
		co.NowFunc = func() time.Time { return c.now }
		co.TimeBetweenExpirationChecks = c.timeBetweenExpirations
	})
}

func (c *CacherSuite) TestGet_NotSet_ReturnsNilAndFalse() {
	key := "my key"
	result, ok := c.cacher.Get(key)
	c.False(ok)
	c.Nil(result)
}

func (c *CacherSuite) TestSetThenGet_RetrievesValue() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value))
	result, ok := c.cacher.Get(key)
	c.True(ok)
	c.Equal(value, string(result))
}

func (c *CacherSuite) TestSetThenGet_TTLPassed__TTLHasNotElapsed_RetrievesValue() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value), 1*time.Minute)
	c.now = c.now.Add(1*time.Minute - 1*time.Millisecond)
	result, ok := c.cacher.Get(key)
	c.True(ok)
	c.Equal(value, string(result))
}

func (c *CacherSuite) TestSetThenGet_TTLPassed__TTLHasElapsed_DoesNotRetrievesValue() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value), 1*time.Minute)
	c.now = c.now.Add(1*time.Minute + 1*time.Millisecond)
	result, ok := c.cacher.Get(key)
	c.False(ok)
	c.Nil(result)
}

func (c *CacherSuite) TestSetThenGet_ExpiryPassed__ExpiryHasNotElapsed_RetrievesValue() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value), c.now.Add(1*time.Minute))
	c.now = c.now.Add(1*time.Minute - 1*time.Millisecond)
	result, ok := c.cacher.Get(key)
	c.True(ok)
	c.Equal(value, string(result))
}

func (c *CacherSuite) TestSetThenGet_ExpiryPassed__ExpiryHasElapsed_RetrievesNilAndFalse() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value), c.now.Add(1*time.Minute))
	c.now = c.now.Add(1*time.Minute + 1*time.Millisecond)
	result, ok := c.cacher.Get(key)
	c.False(ok)
	c.Nil(result)
}

func (c *CacherSuite) TestSetThenDeleteThenGet_ReturnsNilAndFalse() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value))
	c.cacher.Delete(key)
	result, ok := c.cacher.Get(key)
	c.False(ok)
	c.Nil(result)
}

func (c *CacherSuite) TestSetThenSetThenGet_ReturnsUpdatedValue() {
	key, oldValue, newValue := "my key", "my value", "new value"
	c.cacher.Set(key, []byte(oldValue))
	c.cacher.Set(key, []byte(newValue))
	result, ok := c.cacher.Get(key)
	c.True(ok)
	c.Equal(newValue, string(result))
}

func (c *CacherSuite) TestSetThenSetWithNewTTL_TTLExpired_ReturnsNilAndFalse() {
	key, value := "my key", "my value"
	c.cacher.Set(key, []byte(value))
	c.cacher.Set(key, []byte(value), 1*time.Minute)
	c.now = c.now.Add(1*time.Minute + 1*time.Millisecond)
	result, ok := c.cacher.Get(key)
	c.False(ok)
	c.Nil(result)
}

func (c *CacherSuite) TestSetThenGetWithMassiveConcurrency_AvoidsDeadlocks() {
	cacher := NewCacher(func(co *CacherOptions) {
		co.TimeBetweenExpirationChecks = 1 * time.Millisecond
	})
	defer cacher.Close()
	numMessages := 1000000
	numWorkers := 10000
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numMessages)
	keyChannel := make(chan int, numMessages)
	defer close(keyChannel)
	// pre-load the channel so we're ready to take off the moment we start
	for i := range numMessages {
		keyChannel <- i
	}
	for range numWorkers {
		go func() {
			for {
				keyInt, ok := <-keyChannel
				if !ok {
					return
				}
				key := fmt.Sprintf("key-%d", keyInt)
				value := fmt.Sprintf("val-%d", keyInt)
				cacher.Set(key, []byte(value), 20*time.Millisecond)
				result, ok := cacher.Get(key)
				c.True(ok)
				c.Equal(value, string(result))
				time.Sleep(21 * time.Millisecond)
				_, ok = cacher.Get(key)
				c.False(ok)
				waitGroup.Done()
			}

		}()
	}
	waitGroup.Wait()
}

func (c *CacherSuite) TestCloseThenGet_Panics() {
	c.Panics(func() {
		c.cacher.Close()
		c.cacher.Get("key")
	})
}

func (c *CacherSuite) TestCloseThenSet_Panics() {
	c.Panics(func() {
		c.cacher.Close()
		c.cacher.Set("key", []byte("value"))
	})
}

func (c *CacherSuite) TestCloseThenDelete_Panics() {
	c.Panics(func() {
		c.cacher.Close()
		c.cacher.Delete("key")
	})
}

func TestCacherSuite(t *testing.T) {
	suite.Run(t, new(CacherSuite))
}

// func TestCache(t *testing.T) {
// 	cache := NewCacher(1 * time.Millisecond)
// 	defer cache.Close()
// 	wg := sync.WaitGroup{}

// 	for i := range 10001 {
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			now := time.Now()
// 			key := fmt.Sprintf("key %d", i)
// 			value := fmt.Sprintf("value %d", i)
// 			randomMsecs := rand.Intn(100)
// 			randomMsecs += 20
// 			cache.Set(
// 				key,
// 				[]byte(value),
// 				now.Add(time.Duration(randomMsecs)*time.Millisecond),
// 			)
// 			result, ok := cache.Get(key)
// 			if !ok {
// 				t.Logf("key not available: %s", key)
// 				t.Fail()
// 			} else if string(result) != value {
// 				t.Logf("Unexpected value: %v", result)
// 				t.Fail()
// 			} else {
// 				t.Logf("It worked %d", i)
// 			}
// 			time.Sleep(time.Duration(randomMsecs+1) * time.Millisecond)
// 			_, ok = cache.Get(key)
// 			if ok {
// 				t.Logf("Got a record when I shouldn't have!")
// 				t.Fail()
// 			}
// 		}(i)
// 	}
// 	wg.Wait()
// }

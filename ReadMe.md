# In-Memory Cache

## What is this?
This is a sample component intended to demonstrate how I would design an efficient, thread-safe
in-memory cache.

## How do I use it?

Use is pretty simple. Instantiate the component like this:

```golang
import "github.com/jfalkenstein/inmemory-cache"
import "time"
import "json"

cache := inmemorycache.NewCacher(func(opt *inmemorycache.CacherOptions){
    opt.DefaultTTL = 12 * time.Hour
})

// Any value in the form of []byte can be Cached
mapToCache := map[string]string{"Hello": "there"}
myValue, _ := json.Marshal(mapToCache)

// Set values using the default TTL
cache.Set("my key", myValue)

// Or use an explicit TTL
cache.Set("my key", myValue, 1 * time.Hour)

// Or use an explicit time
cache.Set("my key", myValue, time.Date(2020, 2, 2, 2, 2, 2, 0, time.UTC))

// Then get the value
retrieved, ok := cache.Get("my key") // ok will be False if the value was not set
```

## How does it work?

This cache is pretty efficient. It's also very concurrency safe. This is powered by a few 
main pieces:

### A sync.Map for key/value storage
This provides guarantees around concurrent access, atomic operations, and performance.
This is where the actual keys and values are stored.

### A skip-list for storing ordered expiration times
One of the challenges of enforcing expiration for cached values is around knowing when to remove
expired values. This could have been implemented a few different ways. The ways I explored 
before arriving at the final version were:

- **Looping through every cached value.** This would work, but doesn't scale very
well. It wastes a lot of cpu cycles and probably wouldn't impress you very much.
- **Keeping a separate map for expirations.** This works for efficient key/value access. However, this DOESN'T work very well when you're trying to query for every
value whose expiration has elapsed. When you don't know the key, it's little different
from the loop-over-everything option.
- **Using an indexed SQLite database.** This was a heavy-handed solution and ultimately didn't operate across threads when used in-memory. I dismissed this solution after some exploration because it Sqlite has certain limitations when 
operating in-memory. Ultimately, an on-disk option would have worked well, but also
wouldn't be true to the spirit of an "in-memory cache".
- **Sorting a list of expirations on-insert.** This would allow pretty fast traversal
from earliest to latest expiration, but it would be a _bear_ on-insert. Making every 
insert an O(n^2) operation would be downright terrible the larger the cache got.

The final version I arrived at was using a **skip-list**. The only 3rd party package
I referenced in this cache (outside of testing tools) was for this skip list component. I could have implemented my own, but that would have blown up the scope of
this solution needlessly and probably isn't what you're looking for.

Why a skip-list?
* Values are ordered by definition, enforced from the moment they're added to the
collection. This avoids the possibility of sorting the whole list every time a value is added. This provides the very fast ordered-traversal I wanted with the sort-every-insert option.
* It's MUCH more efficient to insert or search for values, on the order of O(log n).
This is far preferable to the "sort-every-insert" option I considered.
* Looking for expired nodes is as simple as starting at the beginning and then crawling the linked list until I hit an expiration that is still valid.
* _I really like Redis_ as an in-memory cache and I'm pretty sure this is the structure Redis uses for SortedSets that are very performant.

### A background goroutine to handle the administration of expirations
For this I'm going to credit what I know of how Redis handles key expirations. As
I understand it, Redis does this:
* Periodically, it does a partial crawl of keys to expire and removes about 1/4 of them from the database
* Get operations where the key is expired simply will return Nothing, regardless of 
whether the value is still in the DB or not.

I chose to create a similar situation. Basically, the administration of expirations
works this way:
* There's a go routine that handles messages to add new expirations, delete old ones,
and periodically walk the skip-list to remove values that have expired.
* Since the skip-list structure I'm using is NOT concurrency-safe, only this one
looping, private goroutine does any operations on that list. This is fine, since it
doesn't actually slow or affect the Get/Set/Delete operations for the cache interface.
* The channels for setting and deleting expirations are buffered so that those operations don't get blocked.

In short, I made the decision that it's ok if there are some expired keys still in the cache _for a while_ so long as the user doesn't ever experience that delay.

## How performant is this? How concurrent can it be?
If you look at the final unit test, you'll see that we run a test with 1M set/get 
operations, juggled concurrently between 10K goroutines. This operation completes
successfully, without any deadlocks, and all operations succeed as expected. It 
takes roughly 3-4 seconds on my machine.
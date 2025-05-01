package inmemorycache

import (
	"context"
	"time"
)

type Cacher interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, expiration time.Time) error
	Check(ctx context.Context) (bool, error)
	Close()
}

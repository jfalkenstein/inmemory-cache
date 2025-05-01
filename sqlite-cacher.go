package inmemorycache

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)


const (
	DefaultExpirationCadence = 1 * time.Minute

	tableCreateSql = `
	CREATE TABLE cache (
		key TEXT NOT NULL,
		value BLOB NOT NULL,
		expiration INTEGER NOT NULL
	);`
	keyIndexCreationSql = "CREATE UNIQUE INDEX cache_key ON cache(key);"
	expirationIndexCreationSql = "CREATE INDEX cache_expiration ON cache(expiration ASC NULLS LAST);"
	clearExpiredSql = "DELETE FROM cache WHERE expiration < ?;"
	getKeySql = "SELECT value FROM cache WHERE key = ? AND expiration >= ?;"
	upsertSql = "INSERT OR REPLACE INTO cache(key, value, expiration) VALUES (?, ?, ?);"
)

type SqliteCacherOptions struct {
	ExpirationCheckCadence time.Duration
	ExpirationErrorHandler func(error)
}

type SqlxDb interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	PingContext(ctx context.Context) error
	Close() error
}

type sqliteCacher struct {
	conn SqlxDb
	options SqliteCacherOptions
	now func() time.Time

	stopChannel chan bool
}

func (s *sqliteCacher) Get(ctx context.Context, key string) ([]byte, error) {
	now := s.now().UnixMilli()
	var result []byte
	err := s.conn.GetContext(ctx, &result, getKeySql, key, now)
	if errors.Is(err, sql.ErrNoRows){
		return nil, nil
	}
	return result, err
}

func (s *sqliteCacher) Set(ctx context.Context, key string, value []byte, expiration time.Time) error {
	now := s.now().UnixMilli()
	_, err := s.conn.ExecContext(ctx, upsertSql, key, value, now)
	return err
}

func (s *sqliteCacher) Check(ctx context.Context) (bool, error) {
	err := s.conn.PingContext(ctx)
	return err == nil, err
}
func (s *sqliteCacher) Close() {
	s.stopChannel <- true
	close(s.stopChannel)
	s.conn.Close()
}

func (s *sqliteCacher) expireContinually(){
	go func(){
		ticker := time.NewTicker(s.options.ExpirationCheckCadence)
		for {
			select {
			case <- ticker.C:
				now := s.now().UTC().UnixMilli()
				_, err := s.conn.ExecContext(context.Background(), clearExpiredSql, now)
				if s.options.ExpirationErrorHandler != nil {
					s.options.ExpirationErrorHandler(err)
				}
			case <- s.stopChannel:
				return
			}
		}
	}()
}



func NewSqliteCacher(ctx context.Context, opts ...func(*SqliteCacherOptions)) (Cacher, error) {
	options := SqliteCacherOptions{
		ExpirationCheckCadence: DefaultExpirationCadence,
		ExpirationErrorHandler: DefaultExpirationErrorHandler,
	}
	for _, opt := range opts {
		opt(&options)
	}
	
	connection, err := sqlx.Connect("sqlite3", ":memory:")
	// todo: wrap this error
	if err != nil {
		return nil, err
	}
	if err = setupCacheDb(ctx, connection); err != nil {
		return nil, err
	}
	cacher := sqliteCacher{
		conn: connection,
		options: options,
		now: func() time.Time { return time.Now().UTC()},
		stopChannel: make(chan bool),
	}
	cacher.expireContinually()
	return &cacher, nil
}



func setupCacheDb(ctx context.Context, connection *sqlx.DB) error {
	if _, err := connection.ExecContext(ctx, tableCreateSql); err != nil {
		return err
	}
	if _, err := connection.ExecContext(ctx, keyIndexCreationSql); err != nil {
		return err
	}
	if _, err := connection.ExecContext(ctx, expirationIndexCreationSql); err != nil {
		return err
	}
	return nil
}

func DefaultExpirationErrorHandler(err error){
	log.Printf("Error encountered by SqliteCacher's continuous expiration: %s", err.Error())
}
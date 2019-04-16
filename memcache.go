package appwrap

type Memcache interface {
	Add(item *CacheItem) error
	AddMulti(item []*CacheItem) error
	CompareAndSwap(item *CacheItem) error
	Delete(key string) error
	DeleteMulti(keys []string) error
	Flush() error
	Get(key string) (*CacheItem, error)
	GetMulti(keys []string) (map[string]*CacheItem, error)
	Increment(key string, amount int64, initialValue uint64) (uint64, error)
	IncrementExisting(key string, amount int64) (uint64, error)
	Namespace(ns string) Memcache
	Set(item *CacheItem) error
	SetMulti(item []*CacheItem) error
}

type CacheLocation string
type CacheName string

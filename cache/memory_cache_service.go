package cache

import (
	"database/sql"
	"errors"
	"time"
	"unsafe"
)

type MemoryCacheService struct {
	client *Client
	close  chan struct{}
}

func NewMemoryCacheServiceByConfig(conf CacheConfig) (*MemoryCacheService, error) {
	return NewMemoryCacheService(conf.Size, conf.CleaningEnable, conf.CleaningInterval)
}
func NewMemoryCacheService(size int64, cleaningEnable bool, cleaningInterval time.Duration) (*MemoryCacheService, error) {
	currentSession := &MemoryCacheService{NewClient(size, cleaningEnable), make(chan struct{})}

	// Check record expiration time and remove
	go func() {
		ticker := time.NewTicker(cleaningInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				items := currentSession.client.GetItems()
				items.Range(func(key, value interface{}) bool {
					item := value.(Item)

					if item.Expires < time.Now().UnixNano() {
						k, _ := key.(string)
						currentSession.client.Get(k)
						tx, _ := item.Data.(*sql.Tx)
						tx.Rollback()
					}
					return true
				})

			case <-currentSession.close:
				return
			}
		}
	}()

	return currentSession, nil
}

// Get return value based on the key provided
func (c *MemoryCacheService) Get(key string) (*sql.Tx, error) {
	obj, err := c.client.Read(key)
	if err != nil {
		return nil, err
	}

	item, ok := obj.(Item)
	if !ok {
		return nil, errors.New("can not map object to Item model")
	}

	if item.Expires < time.Now().UnixNano() {
		return nil, nil
	}
	t, ok2 := item.Data.(*sql.Tx)
	if ok2 {
		return t, nil
	} else {
		return nil, nil
	}
}

// Get return value based on the list of keys provided
func (c *MemoryCacheService) GetMany(keys []string) (map[string]*sql.Tx, []string, error) {
	var itemFound map[string]*sql.Tx
	var itemNotFound []string

	for _, key := range keys {
		obj, err := c.client.Read(key)
		if obj == nil && err == nil {
			itemNotFound = append(itemNotFound, key)
		}

		item, ok := obj.(Item)
		if !ok {
			return nil, nil, errors.New("can not map object to Item model")
		}
		t, ok2 := item.Data.(*sql.Tx)
		if ok2 {
			itemFound[key] = t
		}
	}
	return itemFound, itemNotFound, nil
}

// Get return value based on the key provided
func (c *MemoryCacheService) ContainsKey(key string) (bool, error) {
	obj, err := c.client.Read(key)
	if err != nil {
		return false, err
	}

	item, ok := obj.(Item)
	if !ok {
		return false, errors.New("can not map object to Item model")
	}

	if item.Expires < time.Now().UnixNano() {
		return false, nil
	}

	return true, nil
}

// Put new record set key and value
func (c *MemoryCacheService) Put(key string, value *sql.Tx, expire time.Duration) error {
	if expire == 0 {
		expire = 5 * time.Minute
	}

	if err := c.client.Push(key, Item{
		Data:    value,
		Expires: time.Now().Add(expire).UnixNano(),
	}); err != nil {
		return err
	}

	return nil
}

// Expire new value over the key provided
func (c *MemoryCacheService) Expire(key string, expire time.Duration) (bool, error) {
	val, err := c.client.Get(key)
	if err != nil {
		return false, err
	}

	if err := c.client.Push(key, Item{
		Data:    val,
		Expires: time.Now().Add(expire).UnixNano(),
	}); err != nil {
		return false, err
	}

	return true, nil
}

// Remove deletes the key and its value from the cache.
func (c *MemoryCacheService) Remove(key string) (bool, error) {
	if _, err := c.client.Get(key); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (c *MemoryCacheService) Clear() error {
	return nil
}

// Count return number of records
func (c *MemoryCacheService) Count() (int64, error) {
	return int64(c.client.GetNumberOfKeys()), nil
}

func (c *MemoryCacheService) Keys() ([]string, error) {
	return c.client.Getkeys(), nil
}

// GetDBSize method return redis database size
func (c *MemoryCacheService) Size() (int64, error) {
	return int64(unsafe.Sizeof(c.client)), nil
}

// Close closes the cache and frees up resources.
func (c *MemoryCacheService) Close() error {
	c.close <- struct{}{}
	c.client = NewClient(10*1024*1024, true) // 10 * 1024 * 1024 for 10 mb
	return nil
}

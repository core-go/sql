package cache

import "time"

// CacheConfig ...
type CacheConfig struct {
	Size             int64         `mapstructure:"size" json:"size,omitempty"` // byte
	CleaningEnable   bool          `mapstructure:"cleaning_enable" json:"cleaningEnable,omitempty"`
	CleaningInterval time.Duration `mapstructure:"cleaning_interval" json:"cleaningInterval,omitempty"` // nano-second
}

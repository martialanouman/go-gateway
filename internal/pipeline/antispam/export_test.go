package antispam

import "time"

// SetClock pins the clock the category_mismatch counter buckets by, so a test never straddles an hour.
func (s *RedisState) SetClock(now func() time.Time) { s.now = now }

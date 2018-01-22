package cache_test

import (
	"github.com/tobgu/qocache/cache"
	"strconv"
	"testing"
)

func BenchmarkCachePut(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	maxSize := 1000000
	item := testItem{size: 1000}
	for i := 0; i < b.N; i++ {
		c := cache.New(maxSize, 0)

		for i := 0; i < 10000; i++ {
			c.Put(strconv.Itoa(i), item)
		}

		stats := c.Stats()
		if stats.ItemCount != 865 {
			b.Errorf("Unexpected count: %d", stats.ItemCount)
		}
	}
}

func BenchmarkCacheGet(b *testing.B) {
	maxSize := 10000000
	c := cache.New(maxSize, 0)

	item := testItem{size: 10}
	for i := 0; i < 10000; i++ {
		c.Put(strconv.Itoa(i), item)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10000; i++ {
			item, ok := c.Get(strconv.Itoa(i))
			if !ok {
				b.Errorf("Could not fetch %s", item)
			}
		}
	}
}

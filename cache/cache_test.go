package cache_test

import (
	"github.com/tobgu/qocache/cache"
	"strconv"
	"testing"
	"time"
)

func assertTrue(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Error("Expected true")
	}
}

func assertEquals(t *testing.T, expected, actual int) {
	t.Helper()
	if expected != actual {
		t.Errorf("%d != %d", expected, actual)
	}
}

func assertFalse(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Error("Expected false")
	}
}

type testItem struct {
	size int
}

func (ti testItem) ByteSize() int {
	return ti.size
}

func TestBasicPutGet(t *testing.T) {
	in1 := testItem{size: 1}
	in2 := testItem{size: 2}
	c := cache.New(100, 0)
	err := c.Put("1", in1, in1.ByteSize())
	assertTrue(t, err == nil)

	c.Put("2", in2, in2.ByteSize())
	assertTrue(t, err == nil)

	out1, ok := c.Get("1")
	assertTrue(t, ok)
	assertTrue(t, out1.(testItem) == in1)

	out2, ok := c.Get("2")
	assertTrue(t, ok)
	assertTrue(t, out2.(testItem) == in2)

	// Non existing key
	_, ok = c.Get("3")
	assertFalse(t, ok)
}

func TestMaxSizeIsRespected(t *testing.T) {
	maxSize := 1500000
	item := testItem{size: 100000}
	c := cache.New(maxSize, 0)

	for i := 0; i < 100; i++ {
		err := c.Put(strconv.Itoa(i), item, item.ByteSize())
		assertTrue(t, err == nil)
	}

	stats := c.Stats()
	assertTrue(t, stats.ByteSize > maxSize-item.ByteSize())
	assertTrue(t, stats.ByteSize <= maxSize)

	// Later items still present
	_, ok := c.Get("99")
	assertTrue(t, ok)
	_, ok = c.Get("86")
	assertTrue(t, ok)

	// Early items evicted
	_, ok = c.Get("1")
	assertFalse(t, ok)
	_, ok = c.Get("85")
	assertFalse(t, ok)

	assertTrue(t, stats.ItemCount == 14)
}

func TestElementCannotBeInsertedLargerThanMaxSize(t *testing.T) {
	maxSize := 1500000
	item := testItem{size: 100000}
	c := cache.New(maxSize, 0)

	err := c.Put("1", item, item.ByteSize())
	assertTrue(t, err == nil)

	largeItem := testItem{size: maxSize}
	err = c.Put("1", largeItem, largeItem.ByteSize())
	assertTrue(t, err != nil)
}

func TestMaxAgeIsRespected(t *testing.T) {
	maxSize := 1000000
	maxAge := time.Nanosecond
	c := cache.New(maxSize, maxAge)
	baseStats := c.Stats()

	err := c.Put("1", testItem{}, 100)
	assertTrue(t, err == nil)

	stats := c.Stats()
	assertTrue(t, stats.ItemCount == 1)
	assertEquals(t, baseStats.ByteSize+253, stats.ByteSize)

	time.Sleep(1 * time.Millisecond)

	// Item not returned and has been removed from the cache
	_, ok := c.Get("1")
	assertFalse(t, ok)

	stats = c.Stats()
	assertTrue(t, stats.ItemCount == 0)
	assertEquals(t, baseStats.ByteSize, stats.ByteSize)
}

func TestInsertOnAlreadyExistingKeyOverwritesExistingEntry(t *testing.T) {
	maxSize := 1000000
	c := cache.New(maxSize, 0)

	err := c.Put("1", testItem{size: 100}, 100)
	assertTrue(t, err == nil)
	stats := c.Stats()

	err = c.Put("1", testItem{size: 90}, 90)
	assertTrue(t, err == nil)

	// Mew item returned and bookkeeping data updated
	item, ok := c.Get("1")
	assertTrue(t, ok)
	assertEquals(t, 90, item.(testItem).size)

	newStats := c.Stats()
	assertTrue(t, newStats.ItemCount == 1)
	assertEquals(t, stats.ByteSize-10, newStats.ByteSize)
}

func TestLruProperty(t *testing.T) {
	maxSize := 1000000
	c := cache.New(maxSize, 0)

	// Can only fit two of these in cache at any time
	item := testItem{size: 450000}

	c.Put("1", item, item.ByteSize())
	c.Put("2", item, item.ByteSize())
	c.Put("3", item, item.ByteSize())

	_, ok := c.Get("1")
	assertFalse(t, ok)
	_, ok = c.Get("3")
	assertTrue(t, ok)
	_, ok = c.Get("2")
	assertTrue(t, ok)

	c.Put("4", item, item.ByteSize())

	// Because of the order of the previous Gets above we expect
	// "2" to remain in the cache even though it was inserted after
	// "3" since it was touched last of the two.
	_, ok = c.Get("3")
	assertFalse(t, ok)
	_, ok = c.Get("2")
	assertTrue(t, ok)
	_, ok = c.Get("4")
	assertTrue(t, ok)

}

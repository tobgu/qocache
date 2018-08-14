package statistics

import (
	"runtime"
	"sync"
	"time"

	"github.com/tobgu/qocache/cache"
)

type Statistics struct {
	cache      *cache.LruCache
	bufferSize int
	lock       *sync.Mutex
	data       StatisticsData
	dataSince  time.Time
}

type probe struct {
	startTime time.Time
	stats     *Statistics
}

// QueryProbe is used to collect statistics related to querying datasets
type QueryProbe struct {
	probe
}

func (sp QueryProbe) Success() {
	s := sp.stats
	s.lock.Lock()
	if s.sizeOkF(s.data.StoreDurations) {
		s.data.QueryDurations = append(s.data.QueryDurations, time.Now().Sub(sp.startTime).Seconds())
		s.data.HitCount++
	}
	s.lock.Unlock()
}

func (sp QueryProbe) Missing() {
	s := sp.stats
	s.lock.Lock()
	s.data.MissCount++
	s.lock.Unlock()
}

// StoreProbe is used to collect statistics related to storing datasets
type StoreProbe struct {
	probe
}

func (sp StoreProbe) Success(rowCount int) {
	s := sp.stats
	s.lock.Lock()
	s.data.StoreCount++
	if s.sizeOkF(s.data.StoreDurations) {
		s.data.StoreDurations = append(s.data.StoreDurations, time.Now().Sub(sp.startTime).Seconds())
		s.data.StoreRowCounts = append(s.data.StoreRowCounts, rowCount)
	}
	s.lock.Unlock()
}

func New(cache *cache.LruCache, bufferSize int) *Statistics {
	return &Statistics{
		lock:       &sync.Mutex{},
		data:       newStatisticsData(bufferSize),
		dataSince:  time.Now(),
		bufferSize: bufferSize,
		cache:      cache,
	}
}
func newStatisticsData(bufferSize int) StatisticsData {
	return StatisticsData{
		StoreDurations:         make([]float64, 0, bufferSize),
		StoreRowCounts:         make([]int, 0, bufferSize),
		QueryDurations:         make([]float64, 0, bufferSize),
		DurationsUntilEviction: make([]float64, 0, bufferSize),
	}
}

func (s *Statistics) sizeOkF(x []float64) bool {
	return len(x) < s.bufferSize
}

func newProbe(s *Statistics) probe {
	return probe{startTime: time.Now(), stats: s}
}

func (s *Statistics) ProbeStore() StoreProbe {
	return StoreProbe{newProbe(s)}
}

func (s *Statistics) ProbeQuery() QueryProbe {
	return QueryProbe{newProbe(s)}
}

// Memstats is basically a subset of runtime.GoMemStats
type GoMemStats struct {
	NumGC           uint32
	NumForcedGC     uint32
	PauseTotalNs    uint64
	HeapObjects     uint64
	HeapAlloc       uint64
	HeapSys         uint64
	HeapReleased    uint64
	HeapSizeCurrent uint64 // HeapSys - HeapReleased
	Mallocs         uint64
	Frees           uint64
}

type StatisticsData struct {
	DatasetCount           int        `json:"dataset_count"`
	CacheSize              int        `json:"cache_size"`
	HitCount               int        `json:"hit_count"`
	MissCount              int        `json:"miss_count"`
	SizeEvictCount         int        `json:"size_evict_count"`
	AgeEvictCount          int        `json:"size_evict_count"`
	StoreCount             int        `json:"store_count"`
	StatisticsDuration     float64    `json:"statistics_duration"`
	StatisticsBufferSize   int        `json:"statistics_buffer_size"`
	StoreDurations         []float64  `json:"store_durations,omitempty"`
	StoreRowCounts         []int      `json:"store_row_counts,omitempty"`
	QueryDurations         []float64  `json:"query_durations,omitempty"`
	DurationsUntilEviction []float64  `json:"durations_until_eviction,omitempty"`
	GoMemStats             GoMemStats `json:"go_mem_stats"`
}

func durationsToSeconds(d []time.Duration) []float64 {
	result := make([]float64, len(d))
	for i := range result {
		result[i] = d[i].Seconds()
	}
	return result
}

func (s *Statistics) Stats() StatisticsData {
	memStats := getMemstats()
	s.lock.Lock()
	defer s.lock.Unlock()

	now := time.Now()
	cs := s.cache.Stats()

	stats := s.data
	stats.DatasetCount = cs.ItemCount
	stats.CacheSize = cs.ByteSize
	stats.SizeEvictCount = cs.SizeEvictCount
	stats.AgeEvictCount = cs.AgeEvictCount
	stats.DurationsUntilEviction = durationsToSeconds(cs.TimeToEviction)
	stats.StatisticsDuration = now.Sub(s.dataSince).Seconds()
	stats.StatisticsBufferSize = s.bufferSize
	stats.GoMemStats = memStats

	s.data = newStatisticsData(s.bufferSize)
	s.dataSince = now

	return stats
}

func getMemstats() GoMemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return GoMemStats{
		NumGC:           m.NumGC,
		NumForcedGC:     m.NumForcedGC,
		PauseTotalNs:    m.PauseTotalNs,
		HeapObjects:     m.HeapObjects,
		HeapAlloc:       m.HeapAlloc,
		HeapSys:         m.HeapSys,
		HeapReleased:    m.HeapReleased,
		HeapSizeCurrent: m.HeapSys - m.HeapReleased,
		Mallocs:         m.Mallocs,
		Frees:           m.Frees,
	}
}

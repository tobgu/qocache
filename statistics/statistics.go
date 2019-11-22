package statistics

import (
	"context"
	"github.com/tobgu/qocache/cache"
	"runtime"
	"sync"
	"time"
)

// statCtxKey is a convenience type for context access
type statCtxKey string

const statCtxKeyStats statCtxKey = "stats"

// Statistics is a global statistics collection object. Currently protected by a single
// mutex. Shard mutex if this ever becomes a point of contention.
type Statistics struct {
	cache      *cache.LruCache
	bufferSize int
	lock       *sync.Mutex
	data       StatisticsData
	dataSince  time.Time
}

type probe interface {
	register(statistics *Statistics, totalDuration float64)
}

// probeProxy is a helper type to be able to store probes in a context further down the call stack
type probeProxy struct {
	creationTime time.Time
	probe        probe
	stats        *Statistics
}

func (pb *probeProxy) register() {
	if pb.probe != nil {
		pb.probe.register(pb.stats, time.Since(pb.creationTime).Seconds())
	}
}

// QueryProbe is used to collect statistics related to querying datasets
type QueryProbe struct {
	startTime time.Time
	stopTime  time.Time
	isHit     bool
}

func (sp *QueryProbe) Success() {
	sp.isHit = true
	sp.stopTime = time.Now()
}

func (sp *QueryProbe) Missing() {
	sp.isHit = false
}

func (sp *QueryProbe) register(stats *Statistics, totalDuration float64) {
	stats.lock.Lock()
	if sp.isHit {
		stats.data.HitCount++
		if stats.sizeOkF(stats.data.QueryDurations) {
			stats.data.QueryDurations = append(stats.data.QueryDurations, sp.stopTime.Sub(sp.startTime).Seconds())
			stats.data.TotalQueryDurations = append(stats.data.TotalQueryDurations, totalDuration)
		}
	} else {
		stats.data.MissCount++
	}
	stats.lock.Unlock()
}

func NewQueryProbe(ctx context.Context) *QueryProbe {
	p := &QueryProbe{startTime: time.Now()}
	ctx.Value(statCtxKeyStats).(*probeProxy).probe = p
	return p
}

// StoreProbe is used to collect statistics related to storing datasets
type StoreProbe struct {
	startTime time.Time
	stopTime  time.Time
	rowCount  int
	success   bool
}

func (sp *StoreProbe) Success(rowCount int) {
	sp.success = true
	sp.stopTime = time.Now()
	sp.rowCount = rowCount
}

func (sp *StoreProbe) register(stats *Statistics, totalDuration float64) {
	if sp.success {
		stats.lock.Lock()
		stats.data.StoreCount++
		if stats.sizeOkF(stats.data.StoreDurations) {
			stats.data.StoreDurations = append(stats.data.StoreDurations, sp.stopTime.Sub(sp.startTime).Seconds())
			stats.data.TotalStoreDurations = append(stats.data.TotalStoreDurations, totalDuration)
			stats.data.StoreRowCounts = append(stats.data.StoreRowCounts, sp.rowCount)
		}
		stats.lock.Unlock()
	}
}

func NewStoreProbe(ctx context.Context) *StoreProbe {
	p := &StoreProbe{startTime: time.Now()}
	ctx.Value(statCtxKeyStats).(*probeProxy).probe = p
	return p
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
		TotalQueryDurations:    make([]float64, 0, bufferSize),
		TotalStoreDurations:    make([]float64, 0, bufferSize),
	}
}

func (s *Statistics) sizeOkF(x []float64) bool {
	return len(x) < s.bufferSize
}

func (s *Statistics) Init(ctx context.Context) context.Context {
	return context.WithValue(ctx, statCtxKeyStats, &probeProxy{creationTime: time.Now(), stats: s})
}

func (s *Statistics) Register(ctx context.Context) {
	box := ctx.Value(statCtxKeyStats).(*probeProxy)
	box.register()
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
	AgeEvictCount          int        `json:"age_evict_count"`
	ReplaceCount           int        `json:"replace_count"`
	StoreCount             int        `json:"store_count"`
	StatisticsDuration     float64    `json:"statistics_duration"`
	StatisticsBufferSize   int        `json:"statistics_buffer_size"`
	StoreDurations         []float64  `json:"store_durations,omitempty"`
	StoreRowCounts         []int      `json:"store_row_counts,omitempty"`
	QueryDurations         []float64  `json:"query_durations,omitempty"`
	DurationsUntilEviction []float64  `json:"durations_until_eviction,omitempty"`
	GoMemStats             GoMemStats `json:"go_mem_stats"`

	// JSON names differ for compatibility with QCache metric names
	TotalQueryDurations []float64 `json:"query_request_durations"`
	TotalStoreDurations []float64 `json:"store_request_durations"`
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
	newStatData := newStatisticsData(s.bufferSize)
	s.lock.Lock()
	defer s.lock.Unlock()

	now := time.Now()
	cs := s.cache.Stats()
	stats := s.data
	stats.DatasetCount = cs.ItemCount
	stats.CacheSize = cs.ByteSize
	stats.SizeEvictCount = cs.SizeEvictCount
	stats.AgeEvictCount = cs.AgeEvictCount
	stats.ReplaceCount = cs.ReplaceCount
	stats.DurationsUntilEviction = durationsToSeconds(cs.TimeToEviction)
	stats.StatisticsDuration = now.Sub(s.dataSince).Seconds()
	stats.StatisticsBufferSize = s.bufferSize
	stats.GoMemStats = memStats
	s.data = newStatData
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

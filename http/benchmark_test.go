package http_test

import (
	"bytes"
	"fmt"
	"github.com/gocarina/gocsv"
	"io"
	"net/http"
	"testing"
)

type TestDataStrings struct {
	Foo string
	Bar string
	Baz string
	Qux int
}

type TestDataInts struct {
	Foo int
	Bar int
	Baz int
	Qux int
}

func marshalToCsv(data interface{}) *bytes.Reader {
	b := new(bytes.Buffer)
	gocsv.Marshal(data, b)
	return bytes.NewReader(b.Bytes())
}

func createStringCsv(length int) *bytes.Reader {
	data := make([]TestDataStrings, 0, length)

	for i := 0; i < length; i++ {
		str := fmt.Sprintf("a%d", i)
		data = append(data, TestDataStrings{str, str, str, i})
	}

	return marshalToCsv(data)
}

func createIntCsv(length int) *bytes.Reader {
	data := make([]TestDataInts, 0, length)

	for i := 0; i < length; i++ {
		data = append(data, TestDataInts{i, i, i, i})
	}

	return marshalToCsv(data)
}

func BenchmarkStringGcOverhead(b *testing.B) {
	buf := createStringCsv(100000)
	benchmarkLargeCache(b, buf)
}

func BenchmarkIntGcOverhead(b *testing.B) {
	buf := createIntCsv(100000)
	benchmarkLargeCache(b, buf)
}

func benchmarkLargeCache(b *testing.B, buf *bytes.Reader) {
	datasetCount := 100
	testRuns := 10000
	cache := newTestCache(b)

	// Insert data
	for i := 0; i < datasetCount; i++ {
		buf.Seek(0, io.SeekStart)
		rr := cache.insertDataset(fmt.Sprintf("FOO%d", i), map[string]string{"Content-Type": "text/csv"}, buf)

		if rr.Code != http.StatusCreated {
			b.Errorf("Wrong insert status code: got %v want %v", rr.Code, http.StatusCreated)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < testRuns; k++ {
			for j := int(0.9 * float64(datasetCount)); j < datasetCount; j++ {
				rr := cache.queryDataset(
					fmt.Sprintf("FOO%d", j),
					map[string]string{"Accept": "application/json"},
					`{"where":  [">", "Qux", 99000]}`)

				if rr.Code != http.StatusOK {
					b.Errorf("Wrong query status code: got %v want %v", rr.Code, http.StatusOK)
				}
			}
		}
	}
}

/*

gc # @#s #%: #+#+# ms clock, #+#/#/#+# ms cpu, #->#-># MB, # MB goal, # P
where the fields are as follows:
gc #        the GC number, incremented at each GC
@#s         time in seconds since program start
#%          percentage of time spent in GC since program start
#+...+#     wall-clock/CPU times for the phases of the GC
#->#-># MB  heap size at GC start, at GC end, and live heap
# MB goal   goal heap size
# P         number of processors used
The phases are stop-the-world (STW) sweep termination, concurrent
mark and scan, and STW mark termination. The CPU times
for mark/scan are broken down in to assist time (GC performed in
line with allocation), background GC time, and idle GC time.
If the line ends with "(forced)", this GC was forced by a
runtime.GC() call.

GODEBUG=gctrace=1 go test -bench=BenchmarkStringGcOverhead -run=^$ -cpuprofile=query_string.prof
------------------------------------------------------------------------------------------------------------
BenchmarkIntGcOverhead-2   	       1	52433996603 ns/op	11592088792 B/op	 6300286 allocs/op
PASS
ok  	github.com/tobgu/qocache/http	57.627s

Showing top 10 nodes out of 40
      flat  flat%   sum%        cum   cum%
    30.82s 53.40% 53.40%     30.82s 53.40%  runtime._ExternalCode /usr/local/go/src/runtime/proc.go
    11.25s 19.49% 72.89%     11.30s 19.58%  github.com/tobgu/qframe/internal/index.Int.Filter /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/index/index.go
     8.17s 14.15% 87.04%      8.17s 14.15%  github.com/tobgu/qframe/internal/iseries.gt /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/iseries/series.go
     0.80s  1.39% 88.43%      0.80s  1.39%  runtime.memmove /usr/local/go/src/runtime/memmove_amd64.s
     0.69s  1.20% 89.62%      0.71s  1.23%  github.com/tobgu/qframe/vendor/bitbucket.org/weberc2/fastcsv.(*fields).nextUnquotedField /home/tobias/Development/go/src/github.com/tobgu/qframe/vendor/bitbucket.org/weberc2/fastcsv/csv.go
     0.64s  1.11% 90.73%      0.64s  1.11%  strconv.ParseUint /usr/local/go/src/strconv/atoi.go
     0.54s  0.94% 91.67%      4.33s  7.50%  github.com/tobgu/qframe/internal/io.ReadCsv /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/io/csv.go
     0.42s  0.73% 92.39%      1.06s  1.84%  strconv.ParseInt /usr/local/go/src/strconv/atoi.go
     0.38s  0.66% 93.05%      0.38s  0.66%  runtime.memclrNoHeapPointers /usr/local/go/src/runtime/memclr_amd64.s
     0.30s  0.52% 93.57%      1.60s  2.77%  github.com/tobgu/qframe/internal/io.columnToData /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/io/csv.go

gc 74 @51.796s 0%: 0.073+1.8+0.38 ms clock, 0.14+0.22/0.14/1.3+0.76 ms cpu, 675->675->346 MB, 692 MB goal, 2 P
gc 75 @53.343s 0%: 0.15+1.6+0.19 ms clock, 0.30+0.20/0.30/0.90+0.38 ms cpu, 675->675->346 MB, 692 MB goal, 2 P
gc 76 @54.912s 0%: 0.046+1.5+0.33 ms clock, 0.093+0.25/0.077/1.0+0.67 ms cpu, 675->675->346 MB, 692 MB goal, 2 P
gc 77 @56.456s 0%: 0.039+1.1+0.20 ms clock, 0.079+0.15/0.093/0.71+0.40 ms cpu, 675->675->346 MB, 692 MB goal, 2 P

GODEBUG=gctrace=1 go test -bench=BenchmarkStringGcOverhead -run=^$ -cpuprofile=query_string.prof
------------------------------------------------------------------------------------------------------------
BenchmarkStringGcOverhead-2   	       1	61522979558 ns/op	11592188576 B/op	 6300186 allocs/op
PASS
ok  	github.com/tobgu/qocache/http	92.100s

Showing top 10 nodes out of 74
      flat  flat%   sum%        cum   cum%
    24.89s 20.10% 20.10%     24.89s 20.10%  runtime._ExternalCode /usr/local/go/src/runtime/proc.go
    15.89s 12.83% 32.92%     15.89s 12.83%  github.com/tobgu/qframe/internal/iseries.gt /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/iseries/series.go
    15.10s 12.19% 45.12%     15.12s 12.21%  github.com/tobgu/qframe/internal/index.Int.Filter /home/tobias/Development/go/src/github.com/tobgu/qframe/internal/index/index.go
    12.81s 10.34% 55.46%     33.73s 27.23%  runtime.scanobject /usr/local/go/src/runtime/mgcmark.go
        9s  7.27% 62.72%      9.27s  7.48%  runtime.greyobject /usr/local/go/src/runtime/mgcmark.go
     8.75s  7.06% 69.79%      8.75s  7.06%  runtime.heapBitsForObject /usr/local/go/src/runtime/mbitmap.go
     3.77s  3.04% 72.83%      6.53s  5.27%  runtime.evacuate /usr/local/go/src/runtime/hashmap.go
     3.45s  2.79% 75.62%     12.73s 10.28%  runtime.mapassign_faststr /usr/local/go/src/runtime/hashmap_fast.go
     2.31s  1.87% 77.48%      2.31s  1.87%  runtime.greyobject /usr/local/go/src/runtime/mbitmap.go
     2.16s  1.74% 79.23%      2.53s  2.04%  runtime.mapaccess2_faststr /usr/local/go/src/runtime/hashmap_fast.go

gc 42 @55.805s 16%: 1.3+3202+0.076 ms clock, 2.7+373/1588/1606+0.15 ms cpu, 2300->2565->1289 MB, 2595 MB goal, 2 P
gc 43 @63.852s 15%: 0.072+2324+0.12 ms clock, 0.14+434/1169/1151+0.25 ms cpu, 2270->2559->1314 MB, 2579 MB goal, 2 P
gc 44 @70.816s 15%: 0.095+3339+0.13 ms clock, 0.19+347/1671/1674+0.26 ms cpu, 2268->2592->1348 MB, 2628 MB goal, 2 P
gc 45 @78.871s 15%: 0.035+2899+0.30 ms clock, 0.071+140/1441/1464+0.61 ms cpu, 2312->2646->1359 MB, 2697 MB goal, 2 P
gc 46 @86.628s 14%: 2.8+3081+0.52 ms clock, 5.7+183/1543/1544+1.0 ms cpu, 2339->2679->1365 MB, 2718 MB goal, 2 P
gc 47 @94.739s 14%: 0.15+2968+14 ms clock, 0.30+189/1479/1495+29 ms cpu, 2349->2694->1370 MB, 2730 MB goal, 2 P

GODEBUG=gctrace=1 go test -bench=BenchmarkStringGcOverhead -run=^$ -cpuprofile=query_string.prof
------------------------------------------------------------------------------------------------------------
Refactor string series to use one, big, byte blob backing it:
- query times reduced by ~30% (61.5 s -> 44.0 s)
- Load times reduced by a lot! (30 s -> 4 s)

gc 55 @34.935s 0%: 0.040+1.1+0.13 ms clock, 0.080+0.16/0.047/0.84+0.26 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 56 @36.890s 0%: 0.046+1.1+0.37 ms clock, 0.093+0.12/0.041/0.99+0.75 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 57 @38.844s 0%: 0.041+1.4+0.33 ms clock, 0.082+0.12/0.053/1.0+0.67 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 58 @40.814s 0%: 0.067+1.6+0.25 ms clock, 0.13+0.18/0.17/1.2+0.51 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 59 @42.822s 0%: 0.045+1.6+0.27 ms clock, 0.091+0.18/0.24/1.2+0.54 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 60 @44.802s 0%: 0.053+1.4+0.41 ms clock, 0.10+0.27/0.062/1.1+0.82 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 61 @46.782s 0%: 0.037+1.0+0.040 ms clock, 0.075+0.16/0.099/0.79+0.080 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
goos: linux
goarch: amd64
pkg: github.com/tobgu/qocache/http
BenchmarkStringGcOverhead-2   	       1	43963161062 ns/op	11592054216 B/op	 6300206 allocs/op
PASS
ok  	github.com/tobgu/qocache/http	47.863s

go test -bench=BenchmarkStringGcOverhead -run=^$ -cpuprofile=query_string_blob_json_resp.prof
------------------------------------------------------------------------------------------------------------
Modified benchmark to start returning JSON of the last 1000 records

BenchmarkStringGcOverhead-2   	       1	93219389896 ns/op	29468321808 B/op	307800583 allocs/op
PASS
ok  	github.com/tobgu/qocache/http	96.929s

go test -bench=BenchmarkStringGcOverhead -run=^$ -cpuprofile=query_string_blob_json_resp.prof
------------------------------------------------------------------------------------------------------------
Tweaked how JSON is generated to get rid of unnecessary allocations.

gc 81 @72.953s 0%: 0.038+1.2+0.34 ms clock, 0.077+0.19/0.064/0.72+0.68 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 82 @74.576s 0%: 0.037+2.1+0.41 ms clock, 0.075+0.11/0.40/0.85+0.82 ms cpu, 1018->1019->522 MB, 1044 MB goal, 2 P
gc 83 @76.166s 0%: 0.043+1.2+0.31 ms clock, 0.087+0.39/0.031/1.0+0.62 ms cpu, 1019->1019->522 MB, 1045 MB goal, 2 P
gc 84 @77.796s 0%: 1.7+0.83+0.13 ms clock, 3.4+0/0.33/2.1+0.26 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
gc 85 @79.407s 0%: 0.046+1.5+0.28 ms clock, 0.092+0.35/0.037/1.1+0.56 ms cpu, 1018->1018->522 MB, 1044 MB goal, 2 P
goos: linux
goarch: amd64
pkg: github.com/tobgu/qocache/http
BenchmarkStringGcOverhead-2   	       1	77230962327 ns/op	24673037872 B/op	 8100463 allocs/op
PASS
ok  	github.com/tobgu/qocache/http	81.067s


Conclusions
-----------
* From the above and from looking at the profiling result, a lot of work (~30 %) spent in
mark and sweep for datasets with strings in them...
* A sync pool for the int and bool indexes may be worthwhile testing
* Are forced GCs a good idea?
* Can the GC be set tighter to avoid over allocating memory?
*/

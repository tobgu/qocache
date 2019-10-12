package main

import (
	"io/ioutil"
	"net/http"
	"time"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	for i := 0; i < 10; i++ {
		t0 := time.Now()
		resp, err := http.Get("http://localhost:8888/qcache/status")
		panicOnErr(err)

		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}

		b, err := ioutil.ReadAll(resp.Body)
		panicOnErr(err)

		err = resp.Body.Close()
		panicOnErr(err)

		println("Duration: ", time.Since(t0)/time.Microsecond, " mus, Len: ", len(b), " bytes")
	}
}

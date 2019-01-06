package main

import (
	"net/http"
	"io/ioutil"
	"time"
)

func main() {
	for i := 0; i < 10; i++ {
		t0 := time.Now()
		resp, err := http.Get("http://localhost:8888/qcache/status")
		if err != nil {
			panic(err)
		}

		if resp.StatusCode != http.StatusOK {
			panic(resp.StatusCode)
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()

		println("Duration: ", time.Now().Sub(t0) / time.Microsecond, " mus, Len: ", len(b), " bytes")
	}
}

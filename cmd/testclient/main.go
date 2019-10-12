package main

// Simple test client that can be used for load testing. Does not perform very well on MacOS
// where a lot of time is spent in syscalls to write and read data.

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

const header = "a,b,c,d,e,f,g,h\n"
const line = "1200,456,123.12345,a string,another string,9877654.2,1234567.12,77\n"
const lineCount = 5000

func main() {
	buf := &bytes.Buffer{}
	buf.WriteString(header)
	for i := 0; i < lineCount; i++ {
		buf.WriteString(line)
	}
	payload := buf.Bytes()

	client := &http.Client{}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	t0 := time.Now()
	for i := 0; i < 100000000; i++ {
		resp, err := client.Post(fmt.Sprintf("http://localhost:8888/qcache/dataset/foo%d", i), "text/csv", bytes.NewReader(payload))
		if err != nil {
			log.Fatal(err)
		}

		if resp.StatusCode != http.StatusCreated {
			log.Fatal(fmt.Sprintf("Code: %s", resp.Status))
		}

		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}

		err = resp.Body.Close()
		if err != nil {
			log.Fatal(err)
		}

		if i%100 == 0 {
			avgMs := 1000 * time.Since(t0).Seconds() / 100
			fmt.Printf("Total count: %d, mean req time: %.2f ms\n", i, avgMs)
			t0 = time.Now()
		}
	}
}

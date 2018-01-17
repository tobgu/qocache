package main

import (
	qhttp "github.com/tobgu/qocache/http"
	"log"
	"net/http"
)

func main() {
	log.Fatal(http.ListenAndServe(":8888", qhttp.Application()))
}

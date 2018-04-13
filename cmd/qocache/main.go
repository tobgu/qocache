package main

import (
	"fmt"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
	"log"
	"net/http"
)

type Config struct {
	Foo string `mapstructure:"foo"`
}

func main() {
	c, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Configuration error: %s", err.Error())
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", c.Port), qhttp.Application()))
}

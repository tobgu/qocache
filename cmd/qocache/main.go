package qocache

import (
	"fmt"
	"log"
	"net/http"

	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
)

func Main() {
	c, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Configuration error: %s", err.Error())
	}
	address := fmt.Sprintf(":%d", c.Port)
	logger := config.Logger()
	logger.Infof("Qocache is listening @%s", address)
	logger.Fatal(http.ListenAndServe(address, qhttp.Application(c)))
}

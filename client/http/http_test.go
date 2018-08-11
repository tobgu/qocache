package client_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tobgu/qframe"
	chttp "github.com/tobgu/qocache/client/http"
	"github.com/tobgu/qocache/config"
	qhttp "github.com/tobgu/qocache/http"
)

func highPort() int {
	for {
		n := rand.Intn(49151)
		if n >= 1024 && n <= 49151 {
			return n
		}
	}
}

func equal(qf qframe.QFrame, other qframe.QFrame) error {
	equal, msg := qf.Equals(other)
	if equal {
		return nil
	}
	fmt.Println("Original: ")
	fmt.Println(qf)
	fmt.Println("Other: ")
	fmt.Println(other)
	return fmt.Errorf("QFrames are not equal: %s", msg)
}

func TestHTTPClient(t *testing.T) {
	cfg := config.Config{Port: highPort()}
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: qhttp.Application(cfg),
	}
	go func() {
		assert.NoError(t, server.ListenAndServe())
	}()
	qf := qframe.New(map[string]interface{}{
		"A": []float64{1, 2, 3, 4},
		"B": []float64{1, 2, 3, 4},
	})
	client := chttp.New(chttp.Endpoint("http", "127.0.0.1", cfg.Port))
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, client.Create("test", qf))
	other := client.Read("test")
	assert.NoError(t, other.Err)
	assert.NoError(t, equal(qf, other))
	assert.NoError(t, server.Close())
}

func init() {
	fmt.Println(config.SetupLogger("debug"))
	rand.Seed(time.Now().Unix())
}

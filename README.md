Qocache is a Go port of [Qcache](https://github.com/tobgu/qcache).

## Functionality in Qocache that does not exist in Qcache
* Defined enum order
* Key-value fields
* Configuration possible throught environment variables and config
  file in addition to command line arguments.

## Functionality in Qcache still missing in Qocache
* Subqueries in `in` clause
* TLS support, server and client certificates
* Basic auth support
* Graceful termination of clients on SIGTERM

## Client Usage

Qocache implements an HTTP client for reading and writing remote [QFrames](https://github.com/tobgu/qframe).

### Example

```go
package main

import (
	"fmt"

	"github.com/tobgu/qframe"

	client "github.com/tobgu/qocache/client/http"
	"github.com/tobgu/qocache/query"
)

func main() {
	// Create a new QFrame
	qf := qframe.New(map[string]interface{}{
		"A": []float64{1, 2, 3, 4},
		"B": []string{"one", "two", "three", "four"},
	})
	fmt.Println(qf)
	// Create a new HTTP client
	cli := client.New(client.Endpoint("http", "localhost", 8888))
	// Submit the QFrame to the server
	cli.Create("test", qf)
	// Perform a server side query returning column A.
	qf = cli.Query("test", query.NewQuery(
		query.Select([]string{"A"}),
	))
	fmt.Println(qf)
}
```

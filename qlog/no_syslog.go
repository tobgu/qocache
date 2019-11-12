// +build windows nacl plan9

package qlog

import (
	"fmt"
	"io"
)

func getSyslog() (io.Writer, error) {
	return nil, fmt.Errorf("Syslog not supported on this operating system")
}

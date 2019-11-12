// +build linux darwin dragonfly freebsd netbsd openbsd solaris

package qlog

import (
	"io"
	"log/syslog"
)

func getSyslog() (io.Writer, error) {
	return syslog.New(syslog.LOG_NOTICE, "qocache")
}

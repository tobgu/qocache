package qlog

import (
	"io"
	"log"
	"os"
)

type Logger interface {
	Printf(fmt string, args ...interface{})
}

func NewStdLogger(useSysLog bool, logDestination string) *log.Logger {
	if useSysLog {
		logDestination = "syslog"
	}

	flag := log.LstdFlags
	var dest io.Writer = os.Stderr
	if logDestination == "syslog" {
		if w, err := getSyslog(); err == nil {
			log.Println("Logging to syslog, no more lines will appear on stderr after this line")
			dest = w
			flag = 0
		} else {
			log.Printf("Erron setting up syslog: %v, will log to stderr", err)
		}
	} else if logDestination == "stdout" {
		log.Println("Logging to stdout")
		dest = os.Stdout
	} else {
		log.Println("Logging to stderr")
	}

	return log.New(dest, "", flag)
}

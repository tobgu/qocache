package qlog

import (
	"log"
	"os"
)

type Logger interface {
	Printf(fmt string, args ...interface{})
}

func NewStdLogger(useSysLog bool) *log.Logger {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	if useSysLog {
		if w, err := getSyslog(); err == nil {
			logger.Println("Using syslog as destination, no more lines will appear on stderr after this line")
			logger.SetOutput(w)
			logger.SetFlags(0)
		} else {
			logger.Printf("Error configuring syslog, will log to stderr: %v", err)
		}
	}
	return logger
}

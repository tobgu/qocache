package config

import (
	"fmt"
)

type Encoding string

const (
	JsonEncoding = Encoding("JSON")
	CsvEncoding  = Encoding("CSV")
)

func (enc Encoding) AcceptHeader() (string, string) {
	switch enc {
	case JsonEncoding:
		return "Accept", "application/json"
	case CsvEncoding:
		return "Accept", "text/csv"
	}
	panic(fmt.Sprintf("unknown encoding type: %s", enc))
}

func (enc Encoding) ContentTypeHeader() (string, string) {
	switch enc {
	case JsonEncoding:
		return "Content-Type", "application/json"
	case CsvEncoding:
		return "Content-Type", "text/csv"
	}
	panic(fmt.Sprintf("unknown encoding type: %s", enc))
}

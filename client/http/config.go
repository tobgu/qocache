package client

import (
	"fmt"
)

type Config struct {
	Endpoint string
}

type ConfigFunc func(*Config)

func NewConfig(funcs ...ConfigFunc) Config {
	cfg := Config{}
	for _, fn := range funcs {
		fn(&cfg)
	}
	return cfg
}

func Endpoint(scheme, host string, port int) ConfigFunc {
	// TODO: Support HTTPS/TLS
	if scheme != "http" {
		panic(fmt.Sprintf("unsupported scheme: %s", scheme))
	}
	return func(cfg *Config) {
		cfg.Endpoint = fmt.Sprintf("%s://%s:%d", scheme, host, port)
	}
}

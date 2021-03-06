package config

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"strings"
)

/*
  -h --help                     Show this screen
  -p PORT --port=PORT           Port [default: 8888]
  -s MAX_SIZE --size=MAX_SIZE   Max cache size, bytes [default: 1000000000]
  -a MAX_AGE --age=MAX_AGE      Max age of cached item, seconds. 0 = never expire. [default: 0]
  -b BUFFER_SIZE --statistics-buffer-size=BUFFER_SIZE  Number of entries to store in statistics
                                                       ring buffer. [default: 1000]
  -c PATH_TO_CERT --cert-file=PATH_TO_CERT   Path to PEM file containing private key and certificate for SSL
  -ca PATH_TO_CA --ca-file=PATH_TO_CA   Path to CA file, if provided client certificates will be checked against this ca
  -d --debug   Run in debug mode
  -ba <USER>:<PASSWORD> --basic-auth=<USER>:<PASSWORD>   Enable basic auth, requires that SSL is enabled.
*/

type Config struct {
	Size                 int    `mapstructure:"size"`
	Port                 int    `mapstructure:"port"`
	HTTPStatusPort       int    `mapstructure:"http-status-port"`
	Age                  int    `mapstructure:"age"`
	StatisticsBufferSize int    `mapstructure:"statistics-buffer-size"`
	ReadHeaderTimeout    int    `mapstructure:"read-header-timeout"`
	ReadTimeout          int    `mapstructure:"read-timeout"`
	WriteTimeout         int    `mapstructure:"write-timeout"`
	HttpPprof            bool   `mapstructure:"http-pprof"`
	RequestLog           bool   `mapstructure:"request-log"`
	UseSyslog            bool   `mapstructure:"use-syslog"`
	LogDestination       string `mapstructure:"log-destination"`
	CAFile               string `mapstructure:"ca-file"`
	CertFile             string `mapstructure:"cert-file"`
	KeyFile              string `mapstructure:"key-file"`
	BasicAuth            string `mapstructure:"basic-auth"`
}

func init() {
	viper.SetConfigName("qocache-conf")
	viper.AddConfigPath(".")
	viper.SetEnvPrefix("QOCACHE")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	addIntParameter("port", "p", "Port to bind to", 8888)
	addIntParameter("http-status-port", "t", "If set a non-TLS server will be started in addition to qocache which only serves /status, this can be used for health checks and similar", 0)
	addIntParameter("size", "s", "Max cache size in bytes", 1000000000)
	addIntParameter("age", "a", "Max age of cached item in seconds, 0 = never expire", 0)
	addIntParameter("statistics-buffer-size", "b", "Number of items to store in statistics ring buffer", 1000)
	addIntParameter("read-header-timeout", "h", "Timeout in seconds for reading HTTP request headers", 20)
	addIntParameter("read-timeout", "r", "Timeout in seconds for reading request body", 60)
	addIntParameter("write-timeout", "w", "Timeout in seconds for reading request body and writing response", 120)
	addBoolParameter("http-pprof", "If HTTP pprof endpoint should be enabled or not", false)
	addBoolParameter("request-log", "If HTTP request logging should be enabled or not", false)
	addBoolParameter("use-syslog", "If syslog should be used or not, default false => log to stderr (DEPRECATED, use --log-destination instead)", false)
	addStringParameter("log-destination", "Destination for logs, stderr/stdout/syslog (default stderr)", "stderr")
	addStringParameter("ca-file", "Path to CA certificate authority file, if passed in it will be used to verify client certificates", "")
	addStringParameter("cert-file", "Path to file containing certificate and optionally private key for server side TLS", "")
	addStringParameter("key-file", "Path to file containing private key for server side TLS, if not set the key is assumed to be present in cert-file", "")
}

func bindEnv(name string) {
	err := viper.BindEnv(name)
	if err != nil {
		panic(err)
	}
}

func addStringParameter(longName, usage, value string) {
	bindEnv(longName)
	pflag.String(longName, value, usage)
}

func addBoolParameter(longName, usage string, value bool) {
	bindEnv(longName)
	pflag.Bool(longName, value, usage)
}

func addIntParameter(longName, shortName, usage string, value int) {
	bindEnv(longName)
	pflag.IntP(longName, shortName, value, usage)
}

func GetConfig() (Config, error) {
	if err := viper.ReadInConfig(); err != nil {
		// Don't care if the config file is missing
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return Config{}, err
		}
	}

	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	c := Config{}
	if err != nil {
		return c, err
	}

	err = viper.Unmarshal(&c)
	if c.KeyFile == "" {
		c.KeyFile = c.CertFile
	}

	return c, err
}

package config

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var defaultLogger *zap.SugaredLogger

// Logger returns the global default logger
func Logger() *zap.SugaredLogger {
	return defaultLogger
}

// SetupLogger configures the default global logger
func SetupLogger(lvl string) error {
	var level zapcore.Level
	err := level.UnmarshalText([]byte(lvl))
	if err != nil {
		return err
	}
	conf := zap.NewProductionConfig()
	conf.Encoding = "console"
	conf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	conf.Level = zap.NewAtomicLevelAt(level)
	// Pretty colors
	conf.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := conf.Build()
	if err != nil {
		return err
	}
	zap.RedirectStdLog(logger)
	zap.ReplaceGlobals(logger)
	defaultLogger = logger.Sugar()
	return nil
}

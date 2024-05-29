package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger interface {
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

var defaultLogger Logger

func init() {
	defaultLogger = NewLogger(NewOptions())
}

type Options struct {
	LogName    string
	LogLevel   string
	FileName   string
	MaxAge     int
	MaxSize    int
	MaxBackups int
	Compress   bool
}

type Option func(*Options)

func NewOptions(opts ...Option) Options {
	options := Options{
		LogName:    "app",
		LogLevel:   "info",
		FileName:   "app.log",
		MaxAge:     10,
		MaxSize:    100,
		MaxBackups: 3,
		Compress:   true,
	}

	for _, opt := range opts {
		opt(&options)
	}

	return options
}

func WithLogLevel(level string) Option {
	return func(o *Options) {
		o.LogLevel = level
	}
}

func WithFileName(filename string) Option {
	return func(o *Options) {
		o.FileName = filename
	}
}

var Levels = map[string]zapcore.Level{
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"fatal": zapcore.FatalLevel,
}

type zapLoggerWrapper struct {
	*zap.SugaredLogger
	options Options
}

func NewLogger(options Options) Logger {
	w := &zapLoggerWrapper{options: options}
	encoder := w.getEncoder()
	writerSyncer := w.getLogWriter()
	core := zapcore.NewCore(encoder, writerSyncer, Levels[options.LogLevel])
	w.SugaredLogger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	return w
}

func (w *zapLoggerWrapper) getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func (w *zapLoggerWrapper) getLogWriter() zapcore.WriteSyncer {
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   w.options.FileName,
		MaxAge:     w.options.MaxAge,
		MaxSize:    w.options.MaxSize,
		MaxBackups: w.options.MaxBackups,
		Compress:   w.options.Compress,
	})
}

func GetDefaultLogger() Logger {
	return defaultLogger
}

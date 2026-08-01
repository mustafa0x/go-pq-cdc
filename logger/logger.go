package logger

import (
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

type loggerValue struct {
	Logger
}

var _default atomic.Value

func init() {
	_default.Store(loggerValue{Logger: slog.Default()})
}

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

var once sync.Once

func InitLogger(l Logger) {
	once.Do(func() {
		_default.Store(loggerValue{Logger: l})
	})
}

func Debug(msg string, args ...any) {
	_default.Load().(loggerValue).Debug(msg, args...)
}

func Info(msg string, args ...any) {
	_default.Load().(loggerValue).Info(msg, args...)
}

func Warn(msg string, args ...any) {
	_default.Load().(loggerValue).Warn(msg, args...)
}

func Error(msg string, args ...any) {
	_default.Load().(loggerValue).Error(msg, args...)
}

func NewSlog(logLevel slog.Level) Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}

package logger

import (
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
)

func TestDefaultLoggerIsUsable(_ *testing.T) {
	Debug("default logger is usable")
}

type testLogger struct {
	*slog.Logger
}

func TestInitLoggerWhileLogging(_ *testing.T) {
	started := make(chan struct{}, 8)
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			started <- struct{}{}
			for range 100_000 {
				Debug("concurrent logger read")
			}
		}()
	}
	for range 8 {
		<-started
	}
	runtime.Gosched()
	InitLogger(testLogger{slog.New(slog.NewTextHandler(io.Discard, nil))})
	readers.Wait()
}

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type SlotInfoProvider interface {
	Info(ctx context.Context) (*slot.Info, error)
}

type Server interface {
	Listen()
	Shutdown()
}

type server struct {
	slotInfoProvider SlotInfoProvider
	server           http.Server
	cdcConfig        config.Config
	closed           bool
}

func NewServer(cfg config.Config, registry metric.Registry, slotInfoProvider SlotInfoProvider) Server {
	s := &server{
		cdcConfig:        cfg,
		slotInfoProvider: slotInfoProvider,
	}

	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.HandlerFor(registry.Prometheus(), promhttp.HandlerOpts{EnableOpenMetrics: true}))

	mux.HandleFunc("GET /status", s.handleStatus)

	mux.HandleFunc("GET /slot", s.handleSlotInfo)

	if cfg.DebugMode {
		mux.HandleFunc("GET /debug/pprof", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
		mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
	}

	s.server = http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Metric.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	return s
}

func (s *server) Listen() {
	logger.Info(fmt.Sprintf("server starting on port :%d", s.cdcConfig.Metric.Port))

	err := s.server.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) && s.closed {
			logger.Info("server stopped")
			return
		}
		logger.Error("server cannot start", "port", s.cdcConfig.Metric.Port, "error", err)
	}
}

func (s *server) Shutdown() {
	if s == nil {
		return
	}
	s.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err != nil {
		logger.Error("error while api cannot be shutdown", "error", err)
	}
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{"status": "ok"}

	if s.slotInfoProvider != nil {
		info, err := s.slotInfoProvider.Info(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		status["slot"] = info
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		logger.Error("failed to encode status response", "error", err)
	}
}

func (s *server) handleSlotInfo(w http.ResponseWriter, r *http.Request) {
	if s.slotInfoProvider == nil {
		http.Error(w, "slot info not available", http.StatusServiceUnavailable)
		return
	}

	info, err := s.slotInfoProvider.Info(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		logger.Error("failed to encode slot info response", "error", err)
	}
}

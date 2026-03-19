package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paerx/anhebridgedb/internal/auth"
	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/httpapi"
)

func main() {
	var (
		addr              = flag.String("addr", ":8080", "HTTP listen address")
		dataDir           = flag.String("data", "./data", "database data directory")
		configPath        = flag.String("config", "./config/config.json", "config file path")
		schedulerInterval = flag.Duration("scheduler-interval", time.Second, "scheduler scan interval")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	engine, err := db.OpenWithConfig(*dataDir, cfg.Storage.Segment, cfg.Performance, cfg.Storage.StrictRecovery)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer engine.Close()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	engine.StartScheduler(runCtx, *schedulerInterval)
	engine.StartMetricsSampler(runCtx, time.Duration(cfg.Performance.MetricsSampleIntervalSeconds)*time.Second)
	authManager := auth.New(cfg.Auth)

	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(engine, authManager).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("anhebridgedb listening on %s with data dir %s segment[max_bytes=%d max_records=%d]", *addr, *dataDir, cfg.Storage.Segment.MaxBytes, cfg.Storage.Segment.MaxRecords)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cancelRun()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

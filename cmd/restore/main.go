package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/paerx/anhebridgedb/internal/backup"
	"github.com/paerx/anhebridgedb/internal/config"
)

func main() {
	var (
		configPath = flag.String("config", "./config/config.json", "config file containing R2 settings")
		dataDir    = flag.String("data", "./data-restored", "offline restore target directory")
		manifest   = flag.String("manifest", "latest", "R2 manifest object key, or latest")
		force      = flag.Bool("force", false, "preserve and replace a non-empty target directory")
		verify     = flag.Bool("verify", true, "verify HMAC chain by opening restored data with strict recovery")
		workers    = flag.Int("workers", 4, "parallel R2 download workers (1-32)")
		timeout    = flag.Duration("timeout", 2*time.Hour, "overall restore timeout")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	report, err := backup.RestoreFromR2(ctx, cfg, backup.RestoreOptions{
		TargetDir:   *dataDir,
		ManifestKey: *manifest,
		Force:       *force,
		Verify:      *verify,
		Workers:     *workers,
	})
	if err != nil {
		log.Fatalf("restore: %v", err)
	}
	output, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(output))
}

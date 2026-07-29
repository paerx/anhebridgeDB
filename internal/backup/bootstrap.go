package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/paerx/anhebridgedb/internal/config"
)

const restoreActivationMarker = ".anhe-restore-in-progress"

// BootstrapRestoreIfEmpty restores the latest R2 generation before the database
// engine opens. Existing local data is never replaced by this path.
func BootstrapRestoreIfEmpty(
	ctx context.Context,
	cfg config.Config,
	dataDir string,
) (RestoreReport, bool, error) {
	if !cfg.Backup.BootstrapRestore.Enabled {
		return RestoreReport{}, false, nil
	}
	empty, err := bootstrapTargetIsEmpty(dataDir)
	if err != nil {
		return RestoreReport{}, false, err
	}
	if !empty {
		return RestoreReport{}, false, nil
	}
	report, err := RestoreFromR2(ctx, cfg, RestoreOptions{
		TargetDir:   dataDir,
		ManifestKey: cfg.Backup.BootstrapRestore.Manifest,
		Verify:      true,
		Workers:     cfg.Backup.BootstrapRestore.Workers,
	})
	if err != nil {
		return RestoreReport{}, true, err
	}
	return report, true, nil
}

func bootstrapTargetIsEmpty(dataDir string) (bool, error) {
	target, err := filepath.Abs(dataDir)
	if err != nil {
		return false, err
	}
	entries, err := os.ReadDir(target)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() == restoreActivationMarker {
			return false, fmt.Errorf(
				"incomplete bootstrap restore detected at %s; inspect or clear the data volume before restarting",
				target,
			)
		}
	}
	return len(entries) == 0, nil
}

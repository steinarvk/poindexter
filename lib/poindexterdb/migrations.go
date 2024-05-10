package poindexterdb

import (
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func (d *DB) runMigrations() error {
	sourceDriver, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("unable to run migrations: %w", err)
	}

	dbInstance, err := postgres.WithInstance(d.db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("unable to run migrations: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", dbInstance)
	if err != nil {
		return fmt.Errorf("unable to run migrations: %w", err)
	}

	if d.options.isVerbose(1) {
		version, dirty, err := m.Version()
		zap.L().Sugar().Infof("Current version: %d, dirty: %v, err: %v", version, dirty, err)
	}

	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			if d.options.isVerbose(2) {
				zap.L().Sugar().Infof("No migrations to run")
			}
			return nil
		}
		return fmt.Errorf("unable to run migrations: %w", err)
	} else {
		if d.options.isVerbose(2) {
			zap.L().Sugar().Infof("Migrations run successfully")

			version, dirty, err := m.Version()

			zap.L().Sugar().Infof("Current version: %d, dirty: %v, err: %v", version, dirty, err)
		}
	}

	return nil
}

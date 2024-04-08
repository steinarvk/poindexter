package cli

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/steinarvk/recdex/lib/config"
	"github.com/steinarvk/recdex/lib/recdexdb"
	"github.com/steinarvk/recdex/lib/server"
)

func readLinesFromFile(filename string, maxLineLength int) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	log.Printf("scanning %q with max line length %d", filename, maxLineLength)

	// Handle longer lines
	buf := make([]byte, maxLineLength)
	scanner.Buffer(buf, maxLineLength)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

var (
	errNotImplemented = fmt.Errorf("not implemented")
)

func Main() {
	var rootCmd = &cobra.Command{Use: "recdex"}

	var serveCmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.Main()
		},
	}

	var syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Sync a file or directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var watchCmd = &cobra.Command{
		Use:   "watch",
		Short: "Watch and continually sync a directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var configCmd = &cobra.Command{
		Use:   "config",
		Short: "Manage configuration files",
	}

	var serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Manage server config files",
	}

	var createServerCmd = &cobra.Command{
		Use:   "create",
		Short: "Create a new server config file",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var addClientCmd = &cobra.Command{
		Use:   "add-client",
		Short: "Modify a server config file to add a new client",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var grantAccessCmd = &cobra.Command{
		Use:   "grant-access [client] [namespace] [read/write/both]",
		Short: "Grant access to a client",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var directoryCmd = &cobra.Command{
		Use:   "directory",
		Short: "Create a config file for a syncable directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var listClients = &cobra.Command{
		Use:   "list-clients",
		Short: "List clients who have accessed data",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var adminCmd = &cobra.Command{
		Use:   "admin",
		Short: "Admin commands that connect directly to the database",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	var statsCmd = &cobra.Command{
		Use:   "stats",
		Short: "Get statistics on database using direct database access",
		RunE: func(cmd *cobra.Command, args []string) error {
			postgresCreds := recdexdb.PostgresConfig{
				PostgresHost: os.Getenv("PGHOST"),
				PostgresUser: os.Getenv("PGUSER"),
				PostgresDB:   os.Getenv("PGDATABASE"),
				PostgresPass: os.Getenv("PGPASSWORD"),
			}
			params := recdexdb.Params{
				Postgres:  postgresCreds,
				Verbosity: 0,
			}

			configValue := os.Getenv("RECDEX_CONFIG")
			if configValue == "" {
				return fmt.Errorf("RECDEX_CONFIG environment variable not set")
			}

			cfg, err := config.Load(configValue)
			if err != nil {
				return err
			}

			ctx := context.Background()

			db, err := recdexdb.Open(ctx, params, *cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			stats, err := db.GetStats(ctx)
			if err != nil {
				return err
			}

			fmt.Println("NumRecords:", stats.NumRecords)
			fmt.Println("NumIndexingKeys:", stats.NumIndexingKeys)
			fmt.Println("NumIndexingRows:", stats.NumIndexingRows)
			fmt.Println("TotalStorageBytes:", stats.TotalSizeAllRelations)
			fmt.Println("TotalIndexBytes:", stats.TotalSizeAllIndexes)
			fmt.Println("TotalRecordBytes:", stats.TotalLengthAllRecords)
			fmt.Println("MaxRecordLength:", stats.MaxRecordLength)
			fmt.Println()
			fmt.Println("Average record length:", float64(stats.TotalLengthAllRecords)/float64(stats.NumRecords))
			fmt.Println("Average record storage size:", float64(stats.TotalSizeAllRelations)/float64(stats.NumRecords))
			fmt.Println("Expansion factor:", float64(stats.TotalSizeAllRelations)/float64(stats.TotalLengthAllRecords))
			fmt.Println("Average indexing rows per record:", float64(stats.NumIndexingRows)/float64(stats.NumRecords))
			fmt.Println()

			for tableName, tableStats := range stats.TableStats {
				fmt.Println("Table:", tableName)
				fmt.Println("  pg_relation_size (bytes):", tableStats.PgRelationSize)
				fmt.Println("  pg_indexes_size (bytes):", tableStats.PgIndexesSize)
				fmt.Println("  pg_total_relation_size (bytes):", tableStats.PgTotalRelationSize)
				fmt.Println("  n_live_tup (approximate rows):", tableStats.NLiveTuples)
				fmt.Println("  n_dead_tup (approximate rows):", tableStats.NDeadTuples)
			}

			return nil
		},
	}

	var printTestQuerySQLCmd = &cobra.Command{
		Use:   "print-test-query-sql",
		Short: "Print a test query SQL",
		RunE: func(cmd *cobra.Command, args []string) error {
			query, queryargs, err := recdexdb.BuildTestQuery()
			if err != nil {
				return err
			}

			fmt.Println(query)
			fmt.Println(queryargs)

			return nil
		},
	}

	var syncSingleFileCmd = &cobra.Command{
		Use:   "syncfile",
		Short: "Flatten and upload one or more files using direct database access",
		RunE: func(cmd *cobra.Command, args []string) error {
			postgresCreds := recdexdb.PostgresConfig{
				PostgresHost: os.Getenv("PGHOST"),
				PostgresUser: os.Getenv("PGUSER"),
				PostgresDB:   os.Getenv("PGDATABASE"),
				PostgresPass: os.Getenv("PGPASSWORD"),
			}
			params := recdexdb.Params{
				Postgres:  postgresCreds,
				Verbosity: 9,
			}

			namespaceName := "main"

			configValue := os.Getenv("RECDEX_CONFIG")
			if configValue == "" {
				return fmt.Errorf("RECDEX_CONFIG environment variable not set")
			}

			ctx := context.Background()

			cfg, err := config.Load(configValue)
			if err != nil {
				return err
			}

			db, err := recdexdb.Open(ctx, params, *cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			maxLineLength := cfg.Limits.MaxBytesPerRecord + 1

			t00 := time.Now()
			var totalProcessed int64
			var totalInserted int64
			var totalError int64

			for i, filename := range args {
				t0 := time.Now()

				lines, err := readLinesFromFile(filename, maxLineLength)
				if err != nil {
					return fmt.Errorf("error reading files from %q: %w", filename, err)
				}

				if i > 0 {
					totalTimeSoFar := time.Since(t00)
					processingRateSoFar := float64(totalProcessed) / totalTimeSoFar.Seconds()
					estimatedTimeToProcessFile := float64(len(lines)) / processingRateSoFar
					log.Printf("processing rate so far: %.2f/sec estimated time to process file %q: %.2fs", processingRateSoFar, filename, estimatedTimeToProcessFile)
				}

				result, err := db.InsertFlattenedRecords(ctx, namespaceName, lines)
				if err != nil {
					return fmt.Errorf("error inserting records from %q: %w", filename, err)
				}

				duration := time.Since(t0)
				summary := result.Summary

				log.Printf("processed %d entries from %q in %s: %+v", len(lines), filename, duration, summary)
				totalProcessed += int64(len(lines))
				totalInserted += int64(summary.NumInserted)
				totalError += int64(summary.NumError)
			}

			totalDuration := time.Since(t00)
			processingRate := float64(totalProcessed) / totalDuration.Seconds()
			log.Printf("processed %d entries in %s: %d inserted, %d errors, %.2f/s", totalProcessed, totalDuration, totalInserted, totalError, processingRate)

			return nil
		},
	}

	var scratchCmd = &cobra.Command{
		Use:   "scratch",
		Short: "Unstable commands to test functionality",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errNotImplemented
		},
	}

	rootCmd.AddCommand(serveCmd, syncCmd, configCmd, adminCmd, scratchCmd)
	configCmd.AddCommand(serverCmd, directoryCmd)
	syncCmd.AddCommand(watchCmd)
	serverCmd.AddCommand(createServerCmd, addClientCmd, grantAccessCmd)
	adminCmd.AddCommand(listClients, statsCmd)
	scratchCmd.AddCommand(syncSingleFileCmd, printTestQuerySQLCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/steinarvk/poindexter/lib/config"
	"github.com/steinarvk/poindexter/lib/dexclient"
	"github.com/steinarvk/poindexter/lib/poindexterdb"
	"github.com/steinarvk/poindexter/lib/server"
	"github.com/steinarvk/poindexter/lib/syncdir"
	"github.com/steinarvk/poindexter/lib/version"
	"go.uber.org/zap"
)

func readLinesFromFile(filename string, maxLineLength int) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	zap.L().Sugar().Infof("scanning %q with max line length %d", filename, maxLineLength)

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

func mkClientCommandGroup(ctx context.Context) *cobra.Command {
	var clientCmds = &cobra.Command{
		Use:   "client",
		Short: "Client commands",
	}

	var syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Sync a directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			for _, arg := range args {
				if err := syncdir.SyncDirFromConfig(ctx, arg); err != nil {
					return err
				}
			}
			return nil
		},
	}
	clientCmds.AddCommand(syncCmd)

	var watchCmd = &cobra.Command{
		Use:   "watch",
		Short: "Watch and continually sync a directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return syncdir.WatchDirs(context.Background(), args)
		},
	}
	clientCmds.AddCommand(watchCmd)

	configCmd := &cobra.Command{
		Use:   "config",
		Short: "List config files",
		RunE: func(cmd *cobra.Command, args []string) error {
			filenames, err := dexclient.ConfigFilenames(ctx)
			if err != nil {
				return err
			}

			for _, fn := range filenames {
				_, err := os.Stat(fn)
				if err != nil && !os.IsNotExist(err) {
					return err
				}
				exists := err == nil

				status := "exists"
				if !exists {
					status = "missing"
				}

				fmt.Printf("\t[%s]\t%s\n", status, fn)

				return nil
			}

			return nil
		},
	}
	clientCmds.AddCommand(configCmd)

	getClient := func(ctx context.Context, sel dexclient.Selector) (*dexclient.Client, error) {
		cfg, err := dexclient.LoadConfig(ctx)
		if err != nil {
			return nil, err
		}

		return dexclient.New(ctx, cfg, sel)
	}

	getCmd := &cobra.Command{
		Use:   "get [namespace] [ID-or-field] [field-value]?",
		Short: "Get a record",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("expected exactly two arguments")
			}

			namespace := args[0]
			recordID := args[1]

			recordUUID, err := uuid.Parse(recordID)
			if err != nil {
				return fmt.Errorf("ID %q is not a valid UUID: %w", recordID, err)
			}

			client, err := getClient(ctx, dexclient.Selector{
				Namespace:   namespace,
				AccessGroup: "query",
			})
			if err != nil {
				return err
			}

			req, err := client.NewRequest(ctx, "GET", fmt.Sprintf("/query/%s/records/%s/", namespace, recordUUID.String()))
			if err != nil {
				return err
			}

			resp, err := client.Do(ctx, req)
			if err != nil {
				return err
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			var genericResponse interface{}
			if err := json.Unmarshal(body, &genericResponse); err != nil {
				return err
			}

			marshalled, err := json.MarshalIndent(genericResponse, "", "  ")
			if err != nil {
				return err
			}

			os.Stdout.Write(marshalled)
			os.Stdout.Write([]byte("\n"))

			return nil
		},
	}
	clientCmds.AddCommand(getCmd)

	// TODO: basic API client
	// output prettyprinted JSON
	// Get (by record ID or unique field)
	// Put (read from stdin, output record ID)
	// Query (given a friendly-query, output JSON)
	// List (given a friendly-query, list record IDs)
	// Hide (i.e. soft-remove)
	// Override (i.e. soft-update)

	return clientCmds
}

func mkServerCommandGroup(ctx context.Context) *cobra.Command {
	var serverCmds = &cobra.Command{
		Use:   "server",
		Short: "Server commands",
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.Main()
		},
	}

	var runServerCmd = &cobra.Command{
		Use:   "run",
		Short: "Run server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.Main()
		},
	}
	serverCmds.AddCommand(runServerCmd)

	var adminCmd = &cobra.Command{
		Use:   "admin",
		Short: "Admin commands that connect directly to the database",
	}
	serverCmds.AddCommand(adminCmd)

	var statsCmd = &cobra.Command{
		Use:   "stats",
		Short: "Get statistics on database using direct database access",
		RunE: func(cmd *cobra.Command, args []string) error {
			postgresCreds := poindexterdb.PostgresConfig{
				PostgresHost: os.Getenv("PGHOST"),
				PostgresUser: os.Getenv("PGUSER"),
				PostgresDB:   os.Getenv("PGDATABASE"),
				PostgresPass: os.Getenv("PGPASSWORD"),
			}
			params := poindexterdb.Params{
				Postgres:  postgresCreds,
				Verbosity: 0,
			}

			configValue := os.Getenv("POINDEXTER_CONFIG")
			if configValue == "" {
				return fmt.Errorf("POINDEXTER_CONFIG environment variable not set")
			}

			cfg, err := config.Load(configValue)
			if err != nil {
				return err
			}

			ctx := context.Background()

			db, err := poindexterdb.Open(ctx, params, *cfg)
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
	adminCmd.AddCommand(statsCmd)

	return serverCmds
}

func Main() {
	ctx := context.Background()

	zapconfig := zap.NewDevelopmentConfig()
	logger, err := zapconfig.Build()
	if err != nil {
		log.Fatal(err)
	}
	zapconfig.Level.SetLevel(zap.InfoLevel)
	defer logger.Sync()

	// set logging level to info

	zap.ReplaceGlobals(logger)

	var rootCmd = &cobra.Command{Use: "poindexter"}

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := version.GetInfo()
			if err != nil {
				return err
			}

			if info.CommitHash != "" {
				dirtyFlag := ""
				if info.DirtyCommit {
					dirtyFlag = " (dirty)"
				}
				fmt.Printf("Commit:       %s%s\n", info.CommitHash, dirtyFlag)
				fmt.Printf("Commit time:  %s\n", info.CommitTime)
			}
			if info.BinaryHash != "" {
				fmt.Printf("Binary hash:  %s\n", info.BinaryHash)
			}

			return nil
		},
	}

	serverCmds := mkServerCommandGroup(ctx)
	clientCmds := mkClientCommandGroup(ctx)

	rootCmd.AddCommand(serverCmds, clientCmds, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

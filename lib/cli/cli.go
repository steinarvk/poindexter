package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/steinarvk/recdex/lib/server"
)

var (
	errNotImplemented = fmt.Errorf("not implemented")
)

func Main() {
	var rootCmd = &cobra.Command{Use: "recdex"}

	var serveCmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the server",
		RunE: func(cmd *cobra.Command, args []string) error {
			serv, err := server.New()
			if err != nil {
				return err
			}
			return serv.Run()
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

	rootCmd.AddCommand(serveCmd, syncCmd, configCmd, adminCmd)
	configCmd.AddCommand(serverCmd, directoryCmd)
	syncCmd.AddCommand(watchCmd)
	serverCmd.AddCommand(createServerCmd, addClientCmd, grantAccessCmd)
	adminCmd.AddCommand(listClients)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

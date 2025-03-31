package main

import (
	"context"
	"log"

	"github.com/hashmap-kz/pgdump-each/internal/common"
	"github.com/hashmap-kz/pgdump-each/internal/dump"
	"github.com/hashmap-kz/pgdump-each/internal/restore"
	"github.com/hashmap-kz/pgdump-each/internal/version"

	"github.com/spf13/cobra"
)

var (
	connStr       string
	inputPath     string
	outputDir     string
	pgBinPath     string
	exitOnErr     bool
	compress      string
	parallelDBS   int
	restoreLogDir string
)

func main() {
	// root

	rootCmd := &cobra.Command{
		Use:          "pgdump-each",
		Short:        "PostgreSQL backup and restore utility",
		Version:      version.Version,
		SilenceUsage: true,
	}

	rootCmd.PersistentFlags().StringVarP(&connStr, "connstr", "c", "", `
PostgreSQL connection string (required)
postgres://user:pass@host:port?sslmode=disable
`)

	rootCmd.PersistentFlags().StringVarP(&pgBinPath, "pgbin-path", "b", "", `
Explicitly specify the path to PostgreSQL binaries (optional)
/usr/lib/postgresql/17/bin
`)

	rootCmd.PersistentFlags().IntVarP(&parallelDBS, "parallel-databases", "p", 2, "Number of concurrent dumps")

	if err := rootCmd.MarkPersistentFlagRequired("connstr"); err != nil {
		log.Fatal(err)
	}

	// backup

	dumpCmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump all databases",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx := context.Background()
			if err := common.SetupEnv(ctx, connStr); err != nil {
				return err
			}
			return dump.RunDumpJobs(ctx, &dump.ClusterDumpContext{
				ConnStr:     connStr,
				OutputDir:   outputDir,
				PgBinPath:   pgBinPath,
				Compress:    compress,
				ParallelDBS: parallelDBS,
			})
		},
	}
	dumpCmd.Flags().StringVarP(&outputDir, "output", "D", "", "Directory to store backups (required)")
	dumpCmd.Flags().StringVarP(&compress, "compress", "Z", "0", "Specify the compression method and/or the compression level to use")
	if err := dumpCmd.MarkFlagRequired("output"); err != nil {
		log.Fatal(err)
	}

	// restore

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore all databases from input",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx := context.Background()
			if err := common.SetupEnv(ctx, connStr); err != nil {
				return err
			}
			return restore.RunRestoreJobs(ctx, &restore.ClusterRestoreContext{
				ConnStr:     connStr,
				InputDir:    inputPath,
				PgBinPath:   pgBinPath,
				ExitOnError: exitOnErr,
				ParallelDBS: parallelDBS,
				LogDir:      restoreLogDir,
			})
		},
	}
	restoreCmd.Flags().StringVarP(&inputPath, "input", "D", "", "Path to backup directory (required)")
	restoreCmd.Flags().BoolVarP(&exitOnErr, "exit-on-error", "e", true, "Exit if an error is encountered while sending SQL commands to the database")
	restoreCmd.Flags().StringVar(&restoreLogDir, "log-dir", "", "Specify where to save restore logs (i.e. /tmp)")
	if err := restoreCmd.MarkFlagRequired("input"); err != nil {
		log.Fatal(err)
	}

	// runner

	rootCmd.AddCommand(dumpCmd, restoreCmd)
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

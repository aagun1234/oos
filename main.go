package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger // Global logger instance

func setupLogger(logConfig LogConfig) error {
	logger = logrus.New()

	// Set output
	if logConfig.OutputFile != "" {
		file, err := os.OpenFile(logConfig.OutputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			logger.SetOutput(io.MultiWriter(os.Stdout, file)) // Log to both console and file
		} else {
			logger.SetOutput(os.Stdout) // Fallback to console if file fails
			fmt.Printf("Failed to log to file %s, using default stdout: %v\n", logConfig.OutputFile, err)
		}
	} else {
		logger.SetOutput(os.Stdout)
	}

	// Set format
	if logConfig.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		// Default to TextFormatter with full timestamp
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	// Set level
	level, err := logrus.ParseLevel(logConfig.Level)
	if err != nil {
		logger.SetLevel(logrus.InfoLevel) // Default to Info if level parsing fails
		logger.Warnf("Invalid log level '%s', defaulting to 'info'. Error: %v", logConfig.Level, err)
	} else {
		logger.SetLevel(level)
	}
	return nil
}

func main() {
	var cfg Config // Declare the main Config struct

	// 1. Define command-line flags and bind them directly to the Config struct fields.
	//    The default values for these flags are empty/zero, which allows YAML to overwrite.
	//    Only the config file path needs a non-empty default if we want to search for it.
	configFile := flag.String("config", "config.yaml", "Path to the YAML configuration file. Configuration from this file will override command-line arguments.")

	// Source S3 Configuration Flags
	flag.StringVar(&cfg.SourceS3.Endpoint, "src-endpoint", "", "Source S3 endpoint (e.g., s3.amazonaws.com)")
	flag.StringVar(&cfg.SourceS3.AccessKeyID, "src-access-key", "", "Source S3 access key ID")
	flag.StringVar(&cfg.SourceS3.SecretAccessKey, "src-secret-key", "", "Source S3 secret access key")
	flag.StringVar(&cfg.SourceS3.Bucket, "src-bucket", "", "Source S3 bucket name")
	flag.BoolVar(&cfg.SourceS3.UseSSL, "src-ssl", true, "Use SSL for source S3 connection")

	// Destination S3 Configuration Flags
	flag.StringVar(&cfg.DestinationS3.Endpoint, "dest-endpoint", "", "Destination S3 endpoint (e.g., s3.us-west-1.amazonaws.com)")
	flag.StringVar(&cfg.DestinationS3.AccessKeyID, "dest-access-key", "", "Destination S3 access key ID")
	flag.StringVar(&cfg.DestinationS3.SecretAccessKey, "dest-secret-key", "", "Destination S3 secret access key")
	flag.StringVar(&cfg.DestinationS3.Bucket, "dest-bucket", "", "Destination S3 bucket name")
	flag.BoolVar(&cfg.DestinationS3.UseSSL, "dest-ssl", true, "Use SSL for destination S3 connection")

	// Migration Configuration Flags
	flag.IntVar(&cfg.Migration.Concurrency, "concurrency", 0, "Number of concurrent file migrations (default: 5 if not set)")
	flag.StringVar(&cfg.Migration.DBPath, "db-path", "", "Path to SQLite database file for progress tracking (default: migration.db if not set)")

	// Logging Configuration Flags
	flag.StringVar(&cfg.Logging.Level, "log-level", "", "Log level: debug, info, warn, error (default: info if not set)")
	flag.StringVar(&cfg.Logging.OutputFile, "log-file", "", "Path to log file (empty for console only)")
	flag.StringVar(&cfg.Logging.Format, "log-format", "", "Log format: text or json (default: text if not set)")

	flag.Parse() // Parse command-line arguments into `cfg`

	// 2. Load Configuration from YAML file (if specified and exists)
	//    This will overwrite values in `cfg` that are also present in the YAML.
	err := LoadConfig(*configFile, &cfg)
	if err != nil {
		fmt.Printf("Failed to load configuration from %s: %v\n", *configFile, err)
		os.Exit(1)
	}
	if _, err := os.Stat(*configFile); err == nil {
		fmt.Printf("Configuration loaded successfully from %s (overriding command-line arguments).\n", *configFile)
	} else if os.IsNotExist(err) {
		fmt.Printf("Config file %s not found. Proceeding with command-line arguments and default values.\n", *configFile)
	} else {
		fmt.Printf("Error checking config file %s: %v. Proceeding with command-line arguments and default values.\n", *configFile, err)
	}


	// 3. Apply programmatic defaults for any values still empty/zero
	cfg.ApplyDefaults()

	// 4. Setup Logger based on final config
	err = setupLogger(cfg.Logging)
	if err != nil {
		// If logger setup fails, we can't use logger, so fall back to fmt.Printf
		fmt.Printf("Failed to setup logger: %v\n", err)
		os.Exit(1)
	}

	logger.Debugf("Final merged config: %+v", cfg)

	// 5. Validate final configuration
	if err := cfg.Validate(); err != nil {
		logger.Fatalf("Invalid configuration: %v", err)
	}

	// 6. Open database
	db, err := OpenDB(cfg.Migration.DBPath)
	if err != nil {
		logger.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// 7. Initialize Migrator
	migrator, err := NewMigrator(
		cfg.SourceS3,
		cfg.DestinationS3,
		db,
		cfg.Migration.Concurrency,
		logger, // Pass the configured logger
	)
	if err != nil {
		logger.Fatalf("Failed to create migrator: %v", err)
	}

	// 8. Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for OS signals (Ctrl+C, etc.) to gracefully stop migration
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received termination signal. Attempting graceful shutdown...")
		cancel() // Cancel the context to stop workers and listing
	}()

	// 9. Start Migration
	err = migrator.StartMigration(ctx)
	if err != nil {
		logger.Fatalf("Migration failed: %v", err)
	}
	logger.Info("Migration tool finished.")
}
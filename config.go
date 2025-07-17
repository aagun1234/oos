package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// S3Config holds the configuration for an S3 compatible service.
type S3Config struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key"`
	SecretAccessKey string `yaml:"secret_key"`
	UseSSL          bool   `yaml:"use_ssl"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
}

// MigrationConfig holds general migration settings.
type MigrationConfig struct {
	Concurrency int    `yaml:"concurrency"`
	DBPath      string `yaml:"db_path"`
	
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level      string `yaml:"level"`        // e.g., "debug", "info", "warn", "error"
	OutputFile string `yaml:"output_file"`  // Path to log file
	Format     string `yaml:"format"`       // "text" or "json"
}

// Config holds all application configurations.
// Values will be populated first by command line, then overwritten by YAML,
// then finally defaults applied if still empty.
type Config struct {
	SourceS3      S3Config        `yaml:"source_s3"`
	DestinationS3 S3Config        `yaml:"destination_s3"`
	Migration     MigrationConfig `yaml:"migration"`
	Logging       LogConfig       `yaml:"logging"`
}

// LoadConfig reads the configuration from a YAML file.
// It populates the provided cfg struct, allowing YAML to overwrite existing values.
func LoadConfig(configPath string, cfg *Config) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		// If file doesn't exist, it's not necessarily an error,
		// we might just proceed with command-line args and internal defaults.
		if os.IsNotExist(err) {
			return nil // No config file found, proceed with current values
		}
		return fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Unmarshal will overwrite fields in cfg that are present in the YAML.
	// Fields not present in YAML will retain their current values (from CLI or initial defaults).
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config file %s: %w", configPath, err)
	}

	return nil
}

// ApplyDefaults applies default values to the Config struct
// for fields that are still empty/zero after CLI and YAML parsing.
func (c *Config) ApplyDefaults() {
	if c.Migration.Concurrency == 0 {
		c.Migration.Concurrency = 5
	}
	if c.Migration.DBPath == "" {
		c.Migration.DBPath = "migration.db"
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}
}

// Validate checks if essential configuration fields are set.
func (c *Config) Validate() error {
	if c.SourceS3.Endpoint == "" || c.SourceS3.AccessKeyID == "" || c.SourceS3.SecretAccessKey == "" || c.SourceS3.Bucket == "" {
		return fmt.Errorf("missing required source S3 configuration (endpoint, access_key, secret_key, bucket)")
	}
	if c.DestinationS3.Endpoint == "" || c.DestinationS3.AccessKeyID == "" || c.DestinationS3.SecretAccessKey == "" || c.DestinationS3.Bucket == "" {
		return fmt.Errorf("missing required destination S3 configuration (endpoint, access_key, secret_key, bucket)")
	}
	if c.Migration.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be a positive integer")
	}
	return nil
}
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// S3服务配置.
type S3Config struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key"`
	SecretAccessKey string `yaml:"secret_key"`
	UseSSL          bool   `yaml:"use_ssl"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
}

// 并发、数据库位置设置.
type MigrationConfig struct {
	Concurrency int    `yaml:"concurrency"`
	DBPath      string `yaml:"db_path"`
	
}

// 日志设置
type LogConfig struct {
	Level      string `yaml:"level"`        // "debug", "info", "warn", "error"
	OutputFile string `yaml:"output_file"`  // 日志文件路径
	Format     string `yaml:"format"`       // "text" 或者 "json"
}

// C配置文件结构.
type Config struct {
	SourceS3      S3Config        `yaml:"source_s3"`
	DestinationS3 S3Config        `yaml:"destination_s3"`
	Migration     MigrationConfig `yaml:"migration"`
	Logging       LogConfig       `yaml:"logging"`
}

// 读取配置.
func LoadConfig(configPath string, cfg *Config) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("配置文件读取出错 %s: %w", configPath, err)
	}

	// 反序列化
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return fmt.Errorf("配置文件反序列化出错 %s: %w", configPath, err)
	}

	return nil
}

// 配置缺省值
func (c *Config) ApplyDefaults() {
	if c.Migration.Concurrency == 0 {
		c.Migration.Concurrency = 4
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

// 配置参数检查
func (c *Config) Validate() error {
	if c.SourceS3.Endpoint == "" || c.SourceS3.AccessKeyID == "" || c.SourceS3.SecretAccessKey == "" || c.SourceS3.Bucket == "" {
		return fmt.Errorf("源S3服务配置不全 (endpoint, access_key, secret_key, bucket)")
	}
	if c.DestinationS3.Endpoint == "" || c.DestinationS3.AccessKeyID == "" || c.DestinationS3.SecretAccessKey == "" || c.DestinationS3.Bucket == "" {
		return fmt.Errorf("目标S3服务配置不全 (endpoint, access_key, secret_key, bucket)")
	}
	if c.Migration.Concurrency <= 0 {
		return fmt.Errorf("并发设置有误")
	}
	return nil
}
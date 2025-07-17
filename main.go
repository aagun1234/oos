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
			fmt.Printf("日志记录失败 %s, 输出到标准输出: %v\n", logConfig.OutputFile, err)
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
		logger.Warnf("日志等级不正确 '%s', 使用缺省的 'info'. Error: %v", logConfig.Level, err)
	} else {
		logger.SetLevel(level)
	}
	return nil
}

func main() {
	var cfg Config 
	configFile := flag.String("config", "config.yaml", "YAML配置文件路径，配置文件优先级大于命令行。")

	flag.StringVar(&cfg.SourceS3.Endpoint, "src-endpoint", "", "源S3的endpoint (e.g., s3.amazonaws.com)")
	flag.StringVar(&cfg.SourceS3.AccessKeyID, "src-access-key", "", "源S3的access key")
	flag.StringVar(&cfg.SourceS3.SecretAccessKey, "src-secret-key", "", "源S3的secret key")
	flag.StringVar(&cfg.SourceS3.Bucket, "src-bucket", "", "源S3的bucket名")
	flag.BoolVar(&cfg.SourceS3.UseSSL, "src-ssl", true, "是否使用SSL连接源服务")

	flag.StringVar(&cfg.DestinationS3.Endpoint, "dest-endpoint", "", "目标S3的endpoint (e.g., s3.us-west-1.amazonaws.com)")
	flag.StringVar(&cfg.DestinationS3.AccessKeyID, "dest-access-key", "", "目标S3的access key")
	flag.StringVar(&cfg.DestinationS3.SecretAccessKey, "dest-secret-key", "", "目标S3的secret key")
	flag.StringVar(&cfg.DestinationS3.Bucket, "dest-bucket", "", "目标S3的bucket名")
	flag.BoolVar(&cfg.DestinationS3.UseSSL, "dest-ssl", true, "是否使用SSL连接目标服务")

	flag.IntVar(&cfg.Migration.Concurrency, "concurrency", 0, "迁移进程的并发数(缺省: 4)")
	flag.StringVar(&cfg.Migration.DBPath, "db-path", "", "记录迁移状态的SQLite数据库文件名(缺省: migration.db)")

	flag.StringVar(&cfg.Logging.Level, "log-level", "", "日志等级: debug, info, warn, error (缺省: info )")
	flag.StringVar(&cfg.Logging.OutputFile, "log-file", "", "日志文件路径 (如果为空的话，输出到控制台)")
	flag.StringVar(&cfg.Logging.Format, "log-format", "", "日志类型: text 或者 json (缺省: text)")

	flag.Parse() 

	err := LoadConfig(*configFile, &cfg)
	if err != nil {
		fmt.Printf("读取配置文件出错 %s: %v\n", *configFile, err)
		os.Exit(1)
	}
	if _, err := os.Stat(*configFile); err == nil {
		fmt.Printf("完成读取配置文件 %s (将覆盖命令行参数).\n", *configFile)
	} else if os.IsNotExist(err) {
		fmt.Printf("配置文件 %s 不存在 %s. 使用命令行参数或者缺省值.\n", *configFile)
	} else {
		fmt.Printf("配置文件格式有问题 %s: %v. 使用命令行参数或者缺省值.\n", *configFile, err)
	}

	cfg.ApplyDefaults()
	err = setupLogger(cfg.Logging)
	if err != nil {
		fmt.Printf("日志配置出错: %v\n", err)
		os.Exit(1)
	}

	logger.Debugf("配置: %+v", cfg)
	if err := cfg.Validate(); err != nil {
		logger.Fatalf("配置有误: %v", err)
	}

	// 6. Open database
	db, err := OpenDB(cfg.Migration.DBPath)
	if err != nil {
		logger.Fatalf("数据库初始化失败: %v", err)
	}
	defer db.Close()

	migrator, err := NewMigrator(
		cfg.SourceS3,
		cfg.DestinationS3,
		db,
		cfg.Migration.Concurrency,
		logger, // Pass the configured logger
	)
	if err != nil {
		logger.Fatalf("迁移进程初始化失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("退出...")
		cancel() 
	}()

	err = migrator.StartMigration(ctx)
	if err != nil {
		logger.Fatalf("迁移失败: %v", err)
	}
	logger.Info("迁移完成.")
}
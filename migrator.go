package main

import (
	"context"
	"database/sql"
	"fmt"
	//"io" 
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus" 
)

type Migrator struct {
	sourceClient *minio.Client
	destClient   *minio.Client
	sourceBucket string
	destBucket   string
	db           *sql.DB
	concurrency  int
	prefix       string
	logger       *logrus.Logger 
	dryrun       bool
}

func NewMigrator(sourceCfg, destCfg S3Config, db *sql.DB, concurrency int, logger *logrus.Logger, dryrun bool) (*Migrator, error) {
	srcClient, err := minio.New(sourceCfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(sourceCfg.AccessKeyID, sourceCfg.SecretAccessKey, ""),
		Secure: sourceCfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("源S3客户端初始化失败: %w", err)
	}

	destClient, err := minio.New(destCfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(destCfg.AccessKeyID, destCfg.SecretAccessKey, ""),
		Secure: destCfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("目标S3客户端初始化失败: %w", err)
	}

	return &Migrator{
		sourceClient: srcClient,
		destClient:   destClient,
		sourceBucket: sourceCfg.Bucket,
		destBucket:   destCfg.Bucket,
		db:           db,
		prefix:       sourceCfg.Prefix,
		concurrency:  concurrency,
		logger:       logger, 
		dryrun:       dryrun,
	}, nil
}

func (m *Migrator) StartMigration(ctx context.Context) error {
	m.logger.WithFields(logrus.Fields{
		"source_endpoint": m.sourceClient.EndpointURL().Host,
		"source_bucket":   m.sourceBucket,
		"dest_endpoint":   m.destClient.EndpointURL().Host,
		"dest_bucket":     m.destBucket,
		"concurrency":     m.concurrency,
		"prefix":          m.prefix,
	}).Info("开始迁移")
	objPrefix:=m.prefix

	exists, err := m.destClient.BucketExists(ctx, m.destBucket)
	if err != nil {
		return fmt.Errorf("检目标桶存在失败: %w", err)
	}
	if !exists {
		m.logger.WithField("bucket", m.destBucket).Info("目标桶不存在， 试图创建...")
		if !m.dryrun {
			err = m.destClient.MakeBucket(ctx, m.destBucket, minio.MakeBucketOptions{})
			if err != nil {
				return fmt.Errorf("目标桶创建失败 '%s': %w", m.destBucket, err)
			}
			m.logger.WithField("bucket", m.destBucket).Info("目标桶创建成功.")
		} else {
			m.logger.WithField("bucket", m.destBucket).Info("dryrun, 不创建目标桶.")
		}
	}

	objectCh := make(chan minio.ObjectInfo, m.concurrency*2) 
	var wg sync.WaitGroup
	var (
		objectCount int
		totalSize int64
		startTime = time.Now()
	)

	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			m.worker(ctx, workerID, objectCh)
		}(i)
	}

	go func() {
		defer close(objectCh)
		listCtx, cancel := context.WithCancel(ctx)
		defer cancel() 

		opts := minio.ListObjectsOptions{
			Prefix:    objPrefix,
			Recursive: true, 
		}

		lastPrint := time.Now()
		
		for object := range m.sourceClient.ListObjects(listCtx, m.sourceBucket, opts) {
			if object.Err != nil {
				m.logger.WithError(object.Err).Warn("获取待迁移对象清单失败")
				continue
			}
			
			totalSize += object.Size
			
			select {
			case objectCh <- object:
				objectCount++
				
				// Print progress every 5 seconds
				if time.Since(lastPrint) > 5*time.Second {
					elapsed := time.Since(startTime).Seconds()
					bps := float64(totalSize) / elapsed
					
					fmt.Printf("\r已迁移对象: %d/%d (%.2f MB/s)", objectCount, objectCount, bps/(1024*1024))
					lastPrint = time.Now()
				}
			case <-ctx.Done(): // Check if main context cancelled
				m.logger.WithField("objects_listed", objectCount).Info("获取对象清单中止.")
				return
			}
		}
		fmt.Printf("\n")
		m.logger.WithFields(logrus.Fields{
			"total_objects_found": objectCount,
			"total_size": fmt.Sprintf("%.2f MB", float64(totalSize)/(1024*1024)),
		}).Info("完成获取对象清单.")
	}()

	wg.Wait() // Wait for all workers to finish

	m.logger.Info("迁移完成.")

	completed, _ := CountMigratedObjects(m.db, "COMPLETED")
	failed, _ := CountMigratedObjects(m.db, "FAILED")
	skipped, _ := CountMigratedObjects(m.db, "SKIPPED")
	m.logger.WithFields(logrus.Fields{
		"completed": completed,
		"failed":    failed,
		"skipped":   skipped,
	}).Info("迁移统计")
	
	if objectCount > 0 {
		elapsed := time.Since(startTime).Seconds()
		avgSpeed := float64(totalSize) / elapsed / (1024*1024)
		fmt.Printf("\n迁移完成. 总对象数: %d, 平均速度: %.2f MB/s\n", objectCount, avgSpeed)
	}

	return nil
}

func (m *Migrator) worker(ctx context.Context, workerID int, objectCh <-chan minio.ObjectInfo) {
	m.logger.WithField("worker_id", workerID).Info("启动迁移进程.")
	for {
		select {
		case object, ok := <-objectCh:
			if !ok { // Channel关闭，没有其他对象了
				m.logger.WithField("worker_id", workerID).Info("迁移进程结束.")
				return
			}
			m.migrateObject(ctx, object)
		case <-ctx.Done():
			m.logger.WithField("worker_id", workerID).Warn("迁移进程中止.")
			return
		}
	}
}

func (m *Migrator) migrateObject(ctx context.Context, obj minio.ObjectInfo) {
	logFields := logrus.Fields{"object_path": obj.Key}
	m.logger.WithFields(logFields).Debug("处理对象")

	record, found, err := GetMigrationStatus(m.db, obj.Key)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Warn("数据库中对象状态未知，强制迁移.")
	} else if found && record.Status == "COMPLETED" && record.SourceETag == obj.ETag && record.SourceSize == obj.Size {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_etag":  obj.ETag,
			"source_size":  obj.Size,
			"status":       "SKIPPED",
			"reason":       "Already migrated and matches source",
		}).Info("当前对象已迁移，跳过当前对象")

		_ = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: record.DestinationETag, 
			DestinationSize: record.DestinationSize,
			Status:          "SKIPPED",
		})
		return
	} else if found && record.Status == "COMPLETED" && (record.SourceETag != obj.ETag || record.SourceSize != obj.Size) {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"old_source_etag": record.SourceETag,
			"new_source_etag": obj.ETag,
			"old_source_size": record.SourceSize,
			"new_source_size": obj.Size,
			"reason":          "Source object changed, re-migrating",
		}).Info("当前对象已迁移，但是源对象已修改. 尝试再次迁移.")
	} else if found && (record.Status == "FAILED" || record.Status == "SKIPPED") {
		m.logger.WithFields(logFields).WithField("previous_status", record.Status).Info("当前对象尚未完成迁移. 尝试再次迁移.")
	}

	if !m.dryrun {
		sourceObject, err := m.sourceClient.GetObject(ctx, m.sourceBucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("从源桶下载对象失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
			return
		}
		defer sourceObject.Close()

		uploadInfo, err := m.destClient.PutObject(ctx, m.destBucket, obj.Key, sourceObject, obj.Size, minio.PutObjectOptions{
			ContentType:  obj.ContentType,  
			UserMetadata: obj.UserMetadata, 
		})
		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("对象上传到目标桶失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
			return
		}

		if uploadInfo.Size != obj.Size {
			m.logger.WithFields(logFields).WithFields(logrus.Fields{
				"source_size":      obj.Size,
				"destination_size": uploadInfo.Size,
				"issue":            "Size mismatch",
			}).Error("迁移验证失败: 对象大小不对")
			_ = RecordMigration(m.db, MigrationRecord{
				Path:            obj.Key,
				SourceETag:      obj.ETag,
				SourceSize:      obj.Size,
				DestinationETag: uploadInfo.ETag,
				DestinationSize: uploadInfo.Size,
				Status:          "FAILED",
			})
			return
		}

		sourceETag := strings.Trim(obj.ETag, `"`)
		destETag := strings.Trim(uploadInfo.ETag, `"`)
		if sourceETag != "" && destETag != "" && sourceETag != destETag &&
			!strings.Contains(sourceETag, "-") && !strings.Contains(destETag, "-") {
			m.logger.WithFields(logFields).WithFields(logrus.Fields{
				"source_etag": sourceETag,
				"dest_etag":   destETag,
				"issue":       "ETag mismatch",
				"note":        "May be due to multipart upload",
			}).Warn("迁移验证告警: ETag不一致")
		}

		err = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: uploadInfo.ETag,
			DestinationSize: uploadInfo.Size,
			Status:          "COMPLETED",
		})
	} else {
		m.logger.WithFields(logFields).WithField("object", obj.Key).Info("dryrun, 不实际下载/上传对象.")
	}
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("数据库记录迁移成功状态失败")
		// This is a critical error, but migration itself was successful. Log and continue.
	} else {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"size": obj.Size,
			"etag": obj.ETag,
		}).Info("当前对象迁移完成")
	}
}
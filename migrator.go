package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

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
	localPath    string // 本地路径字段
	filelist     string // 文件列表字段
	Direction    string // 迁移方向
	objPath      string // 对象路径字段

}

func NewMigrator(sourceCfg, destCfg S3Config, db *sql.DB, concurrency int, logger *logrus.Logger, dryrun bool, Direction string, localPath string, filelist string, prefix string) (*Migrator, error) {

	var bucketLookupType minio.BucketLookupType
	if sourceCfg.PathStyle {
		bucketLookupType = minio.BucketLookupPath
	} else {
		bucketLookupType = minio.BucketLookupDNS
	}

	srcClient, err := minio.New(sourceCfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(sourceCfg.AccessKeyID, sourceCfg.SecretAccessKey, sourceCfg.Token),
		Secure:       sourceCfg.UseSSL,
		Region:       sourceCfg.Region,
		BucketLookup: bucketLookupType,
	})
	if err != nil {
		return nil, fmt.Errorf("源S3客户端初始化失败: %w", err)
	}

	if destCfg.PathStyle {
		bucketLookupType = minio.BucketLookupPath
	} else {
		bucketLookupType = minio.BucketLookupDNS
	}
	destClient, err := minio.New(destCfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(destCfg.AccessKeyID, destCfg.SecretAccessKey, destCfg.Token),
		Secure:       destCfg.UseSSL,
		Region:       destCfg.Region,
		BucketLookup: bucketLookupType,
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
		prefix:       prefix,
		concurrency:  concurrency,
		logger:       logger,
		dryrun:       dryrun,
		localPath:    localPath,       // 本地路径
		filelist:     filelist,        // 文件列表
		Direction:    Direction,       // 迁移方向
		objPath:      destCfg.ObjPath, //云端路径
	}, nil
}

// S3->S3
// VerifyMigratedObjects 校验已迁移对象
func (m *Migrator) VerifyMigratedObjects(ctx context.Context) error {
	m.logger.Info("开始校验已迁移对象...")

	rows, err := m.db.Query("SELECT object_path, source_etag, source_size, destination_etag, destination_size FROM migrations WHERE status = ?", "COMPLETED")
	if err != nil {
		return fmt.Errorf("查询已迁移对象失败: %w", err)
	}
	defer rows.Close()

	var wg sync.WaitGroup
	objectCh := make(chan MigrationRecord, m.concurrency*2)

	// 启动校验工作线程
	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.verifyWorker(ctx, objectCh)
		}()
	}

	// 从数据库读取对象并发送到通道
	go func() {
		defer close(objectCh)
		for rows.Next() {
			var record MigrationRecord
			err := rows.Scan(&record.Path, &record.SourceETag, &record.SourceSize, &record.DestinationETag, &record.DestinationSize)
			if err != nil {
				m.logger.WithError(err).Error("扫描迁移记录失败")
				continue
			}
			select {
			case objectCh <- record:
			case <-ctx.Done():
				m.logger.Info("校验任务被取消.")
				return
			}
		}
		if err = rows.Err(); err != nil {
			m.logger.WithError(err).Error("遍历迁移记录失败")
		}
	}()

	wg.Wait()
	m.logger.Info("所有已迁移对象校验完成.")
	return nil
}

// verifyWorker 校验工作线程
func (m *Migrator) verifyWorker(ctx context.Context, objectCh <-chan MigrationRecord) {
	for {
		select {
		case record, ok := <-objectCh:
			if !ok {
				return // 通道已关闭
			}
			m.logger.Debugf("校验对象: %s", record.Path)
			switch m.Direction {
			case "s3tos3":
				m.verifyS3ToS3(ctx, record)
			case "tolocal":
				m.verifyS3ToLocal(ctx, record)
			case "fromlocal":
				m.verifyLocalToS3(ctx, record)
			default:
				m.logger.Warnf("不支持的迁移方向 '%s'，跳过校验对象: %s", m.Direction, record.Path)
			}
		case <-ctx.Done():
			m.logger.Info("校验工作线程退出.")
			return
		}
	}
}

// verifyS3ToS3 校验源S3和目标S3对象是否一致
func (m *Migrator) verifyS3ToS3(ctx context.Context, record MigrationRecord) {
	srcObjInfo, err := m.sourceClient.StatObject(ctx, m.sourceBucket, record.Path, minio.StatObjectOptions{})
	if err != nil {
		m.logger.WithError(err).Errorf("获取源S3对象信息失败: %s", record.Path)
		return
	}

	destObjInfo, err := m.destClient.StatObject(ctx, m.destBucket, record.Path, minio.StatObjectOptions{})
	if err != nil {
		m.logger.WithError(err).Errorf("获取目标S3对象信息失败: %s", record.Path)
		return
	}

	if strings.ToUpper(srcObjInfo.ETag) != strings.ToUpper(destObjInfo.ETag) || srcObjInfo.Size != destObjInfo.Size {

		m.logger.Errorf("校验失败: %s (源ETag: %s, 目标ETag: %s, 源Size: %d, 目标Size: %d)",
			record.Path, srcObjInfo.ETag, destObjInfo.ETag, srcObjInfo.Size, destObjInfo.Size)
	}
}

// verifyS3ToLocal 校验源S3和本地文件是否一致
func (m *Migrator) verifyS3ToLocal(ctx context.Context, record MigrationRecord) {
	srcObjInfo, err := m.sourceClient.StatObject(ctx, m.sourceBucket, record.Path, minio.StatObjectOptions{})
	if err != nil {
		m.logger.WithError(err).Errorf("获取源S3对象信息失败: %s", record.Path)
		return
	}

	localFilePath := filepath.Join(m.localPath, record.Path)
	localFile, err := os.Open(localFilePath)
	if err != nil {
		m.logger.WithError(err).Errorf("打开本地文件失败: %s", localFilePath)
		return
	}
	defer localFile.Close()

	localFileInfo, err := localFile.Stat()
	if err != nil {
		m.logger.WithError(err).Errorf("获取本地文件信息失败: %s", localFilePath)
		return
	}

	// 计算本地文件的MD5 ETag
	localFileETag, err := calculateLocalFileMD5(localFilePath)
	if err != nil {
		m.logger.WithError(err).Errorf("计算本地文件MD5失败: %s", localFilePath)
		return
	}

	if strings.ToUpper(srcObjInfo.ETag) != strings.ToUpper(localFileETag) || srcObjInfo.Size != localFileInfo.Size() {

		m.logger.Errorf("校验失败: %s (源ETag: %s, 本地ETag: %s, 源Size: %d, 本地Size: %d)",
			record.Path, srcObjInfo.ETag, localFileETag, srcObjInfo.Size, localFileInfo.Size())
	}
}

// verifyLocalToS3 校验本地文件和目标S3对象是否一致
func (m *Migrator) verifyLocalToS3(ctx context.Context, record MigrationRecord) {
	localFilePath := filepath.Join(m.localPath, record.Path)
	localFile, err := os.Open(localFilePath)
	if err != nil {
		m.logger.WithError(err).Errorf("打开本地文件失败: %s", localFilePath)
		return
	}
	defer localFile.Close()

	localFileInfo, err := localFile.Stat()
	if err != nil {
		m.logger.WithError(err).Errorf("获取本地文件信息失败: %s", localFilePath)
		return
	}

	// 计算本地文件的MD5 ETag
	localFileETag, err := calculateLocalFileMD5(localFilePath)
	if err != nil {
		m.logger.WithError(err).Errorf("计算本地文件MD5失败: %s", localFilePath)
		return
	}

	destObjInfo, err := m.destClient.StatObject(ctx, m.destBucket, record.Path, minio.StatObjectOptions{})
	if err != nil {
		m.logger.WithError(err).Errorf("获取目标S3对象信息失败: %s", record.Path)
		return
	}

	if strings.ToUpper(localFileETag) != strings.ToUpper(destObjInfo.ETag) || localFileInfo.Size() != destObjInfo.Size {
		m.logger.Errorf("校验失败: %s (本地ETag: %s, 目标ETag: %s, 本地Size: %d, 目标Size: %d)",
			record.Path, localFileETag, destObjInfo.ETag, localFileInfo.Size(), destObjInfo.Size)
	}
}

// calculateLocalFileMD5 计算本地文件的MD5 ETag
func calculateLocalFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (m *Migrator) MigrateS3ToS3(ctx context.Context) error {
	m.logger.WithFields(logrus.Fields{
		"source_endpoint": m.sourceClient.EndpointURL().Host,
		"source_bucket":   m.sourceBucket,
		"dest_endpoint":   m.destClient.EndpointURL().Host,
		"dest_bucket":     m.destBucket,
		"concurrency":     m.concurrency,
		"prefix":          m.prefix,
	}).Info("开始迁移")
	objPrefix := m.prefix

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
		totalSize   int64
		startTime   = time.Now()
	)

	// 启动工作线程
	migratedObjs := 0
	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			migratedObjs = migratedObjs + m.S3worker(ctx, workerID, objectCh)
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
		if m.filelist != "" { // 从文件列表中读取对象键

			file, err := os.Open(m.filelist)
			if err != nil {
				m.logger.WithError(err).Fatal("打开文件列表失败")
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				objectKey := scanner.Text()
				object, err := m.sourceClient.StatObject(listCtx, m.sourceBucket, objectKey, minio.StatObjectOptions{})
				if err != nil {
					m.logger.WithField("object", objectKey).WithError(err).Error("检查对象是否存在失败")
					continue
				}
				totalSize += object.Size
				select {
				case objectCh <- object:
					objectCount++

					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已迁移对象: %d/%d (%.2f MB/s)", objectCount, objectCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done():
					m.logger.WithField("objects_listed", objectCount).Info("获取对象清单中止.")
					return
				}

			}
			if err := scanner.Err(); err != nil {
				m.logger.WithError(err).Fatal("读取文件列表失败")

			}
			m.logger.WithField("objects_listed", objectCount).Info("从文件列表中读取对象键完成.")

		} else {
			for object := range m.sourceClient.ListObjects(listCtx, m.sourceBucket, opts) {
				if object.Err != nil {
					m.logger.WithError(object.Err).Warn("获取待迁移对象清单失败")
					continue
				}

				totalSize += object.Size

				select {
				case objectCh <- object:
					objectCount++

					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已迁移对象: %d/%d (%.2f MB/s)", objectCount, objectCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done():
					m.logger.WithField("objects_listed", objectCount).Info("获取对象清单中止.")
					return
				}
			}
		}
		fmt.Printf("\n")
		m.logger.WithFields(logrus.Fields{
			"total_objects_found": objectCount,
			"total_size":          fmt.Sprintf("%.2f MB", float64(totalSize)/(1024*1024)),
		}).Info("完成获取对象清单.")
	}()

	wg.Wait()

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
		avgSpeed := float64(totalSize) / elapsed / (1024 * 1024)
		fmt.Printf("\n迁移完成. 总对象数: %d, 已迁移对象数: %d, 平均速度: %.2f MB/s\n", objectCount, migratedObjs, avgSpeed)

	}

	return nil
}

func (m *Migrator) S3worker(ctx context.Context, workerID int, objectCh <-chan minio.ObjectInfo) int {
	m.logger.WithField("worker_id", workerID).Info("启动迁移进程.")
	migratedObjs := 0
	for {
		select {
		case object, ok := <-objectCh:
			if !ok { // Channel关闭，没有其他对象了
				m.logger.WithField("worker_id", workerID).Info("迁移进程结束.")
				return migratedObjs
			}
			migratedObjs += m.migrateObject(ctx, object)
		case <-ctx.Done():
			m.logger.WithField("worker_id", workerID).Warn("迁移进程中止.")
			return migratedObjs
		}
	}

}

func (m *Migrator) migrateObject(ctx context.Context, obj minio.ObjectInfo) int {

	logFields := logrus.Fields{"object_path": obj.Key}
	m.logger.WithFields(logFields).Debug("处理对象")

	record, found, err := GetMigrationStatus(m.db, obj.Key)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Warn("数据库中对象状态未知，强制迁移.")
	} else if found && record.Status == "COMPLETED" && record.SourceETag == obj.ETag && record.SourceSize == obj.Size {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_etag": obj.ETag,
			"source_size": obj.Size,
			"status":      "SKIPPED",
			"reason":      "Already migrated and matches source",
		}).Info("当前对象已迁移，跳过当前对象")

		_ = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: record.DestinationETag,
			DestinationSize: record.DestinationSize,
			Status:          "SKIPPED",
		})
		return 0
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
			return 0

		}
		defer sourceObject.Close()

		uploadInfo, err := m.destClient.PutObject(ctx, m.destBucket, obj.Key, sourceObject, obj.Size, minio.PutObjectOptions{
			ContentType:  obj.ContentType,
			UserMetadata: obj.UserMetadata,
		})
		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("对象上传到目标桶失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
			return 0

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
			return 0

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

	} else {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"size": obj.Size,
			"etag": obj.ETag,
		}).Info("当前对象迁移完成")
	}
	return 1
}

func (m *Migrator) MigrateToLocal(ctx context.Context) error {
	m.logger.WithFields(logrus.Fields{
		"source_endpoint": m.sourceClient.EndpointURL().Host,
		"source_bucket":   m.sourceBucket,
		"local_path":      m.localPath,
		"concurrency":     m.concurrency,
		"prefix":          m.prefix,
	}).Info("开始从S3迁移到本地")

	objPrefix := m.prefix

	// 确保本地目录存在
	if err := os.MkdirAll(m.localPath, 0755); err != nil {
		return fmt.Errorf("创建本地目录失败: %w", err)
	}

	objectCh := make(chan minio.ObjectInfo, m.concurrency*2)
	var wg sync.WaitGroup
	var (
		objectCount int
		totalSize   int64
		startTime   = time.Now()
	)

	// 启动工作线程
	migratedObjs := 0
	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			migratedObjs = migratedObjs + m.localWorker(ctx, workerID, objectCh)
		}(i)
	}

	// 列出所有对象并发送到通道
	go func() {
		defer close(objectCh)
		listCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		opts := minio.ListObjectsOptions{
			Prefix:    objPrefix,
			Recursive: true,
		}

		lastPrint := time.Now()
		if m.filelist != "" {
			// 从文件列表中读取对象键
			file, err := os.Open(m.filelist)
			if err != nil {
				m.logger.WithError(err).Fatal("打开文件列表失败")
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				objectKey := scanner.Text()
				object, err := m.sourceClient.StatObject(listCtx, m.sourceBucket, objectKey, minio.StatObjectOptions{})
				if err != nil {
					m.logger.WithField("object", objectKey).WithError(err).Error("检查对象是否存在失败")
					continue
				}
				totalSize += object.Size
				select {
				case objectCh <- object:
					objectCount++

					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已迁移对象: %d/%d (%.2f MB/s)", objectCount, objectCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done():
					m.logger.WithField("objects_listed", objectCount).Info("获取对象清单中止.")
					return
				}

			}
			if err := scanner.Err(); err != nil {
				m.logger.WithError(err).Fatal("读取文件列表失败")

			}
			m.logger.WithField("objects_listed", objectCount).Info("从文件列表中读取对象键完成.")

		} else {
			for object := range m.sourceClient.ListObjects(listCtx, m.sourceBucket, opts) {
				if object.Err != nil {
					m.logger.WithError(object.Err).Warn("获取待迁移对象清单失败")
					continue
				}

				totalSize += object.Size

				select {
				case objectCh <- object:
					objectCount++

					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已迁移对象: %d/%d (%.2f MB/s)", objectCount, objectCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done():
					m.logger.WithField("objects_listed", objectCount).Info("获取对象清单中止.")
					return
				}
			}
		}
		fmt.Printf("\n")
		m.logger.WithFields(logrus.Fields{
			"total_objects_found": objectCount,
			"total_size":          fmt.Sprintf("%.2f MB", float64(totalSize)/(1024*1024)),
		}).Info("完成获取对象清单.")
	}()

	wg.Wait() // 等待所有工作线程完成

	m.logger.Info("迁移到本地完成.")

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
		avgSpeed := float64(totalSize) / elapsed / (1024 * 1024)
		fmt.Printf("\n迁移完成. 总对象数: %d, 已迁移对象数: %d, 平均速度: %.2f MB/s\n", objectCount, migratedObjs, avgSpeed)

	}

	return nil
}

func (m *Migrator) localWorker(ctx context.Context, workerID int, objectCh <-chan minio.ObjectInfo) int {
	m.logger.WithField("worker_id", workerID).Info("启动本地迁移进程.")
	migratedObjs := 0
	for {
		select {
		case object, ok := <-objectCh:
			if !ok { // 通道关闭，没有其他对象了
				m.logger.WithField("worker_id", workerID).Info("本地迁移进程结束.")
				return migratedObjs
			}
			migratedObjs += m.migrateObjectToLocal(ctx, object)
		case <-ctx.Done():
			m.logger.WithField("worker_id", workerID).Warn("本地迁移进程中止.")
			return migratedObjs
		}
	}
}

func (m *Migrator) migrateObjectToLocal(ctx context.Context, obj minio.ObjectInfo) int {

	logFields := logrus.Fields{"object_path": obj.Key}
	m.logger.WithFields(logFields).Debug("处理对象")

	// 检查迁移状态
	record, found, err := GetMigrationStatus(m.db, obj.Key)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Warn("数据库中对象状态未知，强制迁移.")
	} else if found && record.Status == "COMPLETED" && record.SourceETag == obj.ETag && record.SourceSize == obj.Size {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_etag": obj.ETag,
			"source_size": obj.Size,
			"status":      "SKIPPED",
			"reason":      "Already migrated and matches source",
		}).Info("当前对象已迁移，跳过当前对象")

		_ = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: record.DestinationETag,
			DestinationSize: record.DestinationSize,
			Status:          "SKIPPED",
		})
		return 0

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
	var written int64 = 0
	if !m.dryrun {
		// 构建本地文件路径
		localPath := filepath.Join(m.localPath, obj.Key)

		// 创建本地目录结构
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("创建本地目录失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
			return 0

		}

		// 检查文件是否存在并获取已下载大小(断点续传)
		var offset int64 = 0
		fileInfo, err := os.Stat(localPath)
		if err == nil {
			offset = fileInfo.Size()
			m.logger.WithField("object", obj.Key).Debugf("发现已下载部分文件(%d bytes), 对象大小: %d", offset, obj.Size)

		} else {
			offset = -1
		}

		// 从S3下载对象
		opts := minio.GetObjectOptions{}
		if obj.Size != offset {
			if obj.Size > offset && offset > 0 {

				opts.SetRange(offset, 0) // 设置Range头实现断点续传
				m.logger.WithField("object", obj.Key).Infof("设置Range头实现断点续传, 从第%d字节开始下载", offset)

			} else {
				offset = 0
			}

			reader, err := m.sourceClient.GetObject(ctx, m.sourceBucket, obj.Key, opts)
			if err != nil {
				m.logger.WithFields(logFields).WithError(err).Error("从源桶下载对象失败")
				_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
				return 0

			}
			defer reader.Close()

			// 打开文件(追加模式)
			file, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				m.logger.WithFields(logFields).WithError(err).Error("打开本地文件失败")
				_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
				return 0

			}
			defer file.Close()

			// 复制数据
			_, err = file.Seek(offset, io.SeekStart)
			if err != nil {
				m.logger.WithFields(logFields).WithError(err).Error("设置文件指针失败")
				_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
				return 0
			}
			// 复制数据
			written, err = io.Copy(file, reader)
			if err != nil {
				m.logger.WithFields(logFields).WithError(err).Error("复制数据失败")
				_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
				return 0

			}
		} else {
			m.logger.WithFields(logFields).WithField("object", obj.Key).Infof("文件已存在, 跳过下载, 已下载大小: %d, 对象大小: %d", offset, obj.Size)
			written = 0
		}

		// 验证文件大小
		totalWritten := offset + written
		if totalWritten != obj.Size {
			m.logger.WithFields(logFields).WithFields(logrus.Fields{
				"source_size":      obj.Size,
				"destination_size": totalWritten,
				"issue":            "Size mismatch",
			}).Error("迁移验证失败: 对象大小不对")
			_ = RecordMigration(m.db, MigrationRecord{
				Path:            obj.Key,
				SourceETag:      obj.ETag,
				SourceSize:      obj.Size,
				DestinationETag: "", // 本地文件没有ETag
				DestinationSize: totalWritten,
				Status:          "FAILED",
			})
			return 0

		}

		// 记录迁移成功
		err = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: "", // 本地文件没有ETag
			DestinationSize: totalWritten,
			Status:          "COMPLETED",
		})

		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("数据库记录迁移成功状态失败")
			// 这是一个关键错误，但迁移本身是成功的。记录并继续。
		} else {
			m.logger.WithFields(logFields).WithFields(logrus.Fields{
				"size": obj.Size,
				"etag": obj.ETag,
			}).Info("当前对象迁移到本地完成")
		}

	} else {
		m.logger.WithFields(logFields).WithField("object", obj.Key).Info("dryrun, 不实际下载对象.")
	}
	return 1
}

// MigrateFromLocal 从本地磁盘迁移文件到对象存储
func (m *Migrator) MigrateFromLocal(ctx context.Context) error {
	m.logger.WithFields(logrus.Fields{
		"local_path":    m.localPath,
		"dest_endpoint": m.destClient.EndpointURL().Host,
		"dest_bucket":   m.destBucket,
		"concurrency":   m.concurrency,
		"prefix":        m.prefix,
	}).Info("开始从本地磁盘迁移到对象存储")

	// 确保目标桶存在
	exists, err := m.destClient.BucketExists(ctx, m.destBucket)
	if err != nil {
		return fmt.Errorf("检查目标桶存在失败: %w", err)
	}
	if !exists {
		m.logger.WithField("bucket", m.destBucket).Info("目标桶不存在，试图创建...")
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

	// 创建文件通道和工作组
	fileCh := make(chan string, m.concurrency*2)
	var wg sync.WaitGroup
	var (
		fileCount int
		totalSize int64
		startTime = time.Now()
	)

	// 启动工作线程
	migratedObjs := 0
	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			migratedObjs = migratedObjs + m.fromLocalWorker(ctx, workerID, fileCh)
		}(i)
	}

	// 遍历本地文件并发送到通道
	go func() {
		defer close(fileCh)
		lastPrint := time.Now()

		// 如果指定了文件列表，从文件列表中读取
		if m.filelist != "" {
			file, err := os.Open(m.filelist)
			if err != nil {
				m.logger.WithError(err).Fatal("打开文件列表失败")
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				filePath := scanner.Text()
				// 构建完整的本地文件路径
				fullPath := filepath.Join(m.localPath, filePath)

				// 检查文件是否存在
				fileInfo, err := os.Stat(fullPath)
				if err != nil {
					m.logger.WithField("file", fullPath).WithError(err).Error("检查文件是否存在失败")
					continue
				}

				// 跳过目录
				if fileInfo.IsDir() {
					continue
				}

				totalSize += fileInfo.Size()

				select {
				case fileCh <- filePath:
					fileCount++

					// 每5秒打印进度
					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已上传文件: %d/%d (%.2f MB/s)", fileCount, fileCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done(): // 检查上下文是否取消
					m.logger.WithField("files_listed", fileCount).Info("获取文件清单中止.")
					return
				}
			}

			if err := scanner.Err(); err != nil {
				m.logger.WithError(err).Fatal("读取文件列表失败")
			}

			m.logger.WithField("files_listed", fileCount).Info("从文件列表中读取文件完成.")
		} else {
			// 否则，遍历本地目录
			err := filepath.Walk(m.localPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					m.logger.WithField("path", path).WithError(err).Error("遍历目录失败")
					return nil // 继续遍历
				}

				// 跳过目录
				if info.IsDir() {
					return nil
				}
				// 计算相对路径作为对象键
				relPath, err := filepath.Rel(m.localPath, path)
				if err != nil {
					m.logger.WithField("path", path).WithError(err).Error("计算相对路径失败")
					return nil
				}
				// 转换Windows路径分隔符为UNIX风格（对象存储使用/）
				objKey := filepath.ToSlash(relPath)
				if objKey == "." {
					return nil
				}
				// 前缀过滤
				if m.prefix != "" && !strings.HasPrefix(objKey, m.prefix) {
					return nil
				}
				if !info.IsDir() {
					totalSize += info.Size()
				}

				select {
				case fileCh <- objKey:
					fileCount++

					if time.Since(lastPrint) > 5*time.Second {
						elapsed := time.Since(startTime).Seconds()
						bps := float64(totalSize) / elapsed

						fmt.Printf("\r已上传文件: %d/%d (%.2f MB/s)", fileCount, fileCount, bps/(1024*1024))
						lastPrint = time.Now()
					}
				case <-ctx.Done():
					m.logger.WithField("files_listed", fileCount).Info("获取文件清单中止.")
					return filepath.SkipAll
				}

				return nil
			})

			if err != nil {
				m.logger.WithError(err).Error("遍历本地目录失败")
			}
		}

		fmt.Printf("\n")
		m.logger.WithFields(logrus.Fields{
			"total_files_found": fileCount,
			"total_size":        fmt.Sprintf("%.2f MB", float64(totalSize)/(1024*1024)),
		}).Info("完成获取文件清单.")
	}()

	wg.Wait() // 等待所有工作线程完成

	m.logger.Info("从本地迁移到对象存储完成.")

	completed, _ := CountMigratedObjects(m.db, "COMPLETED")
	failed, _ := CountMigratedObjects(m.db, "FAILED")
	skipped, _ := CountMigratedObjects(m.db, "SKIPPED")
	m.logger.WithFields(logrus.Fields{
		"completed": completed,
		"failed":    failed,
		"skipped":   skipped,
	}).Info("迁移统计")

	if fileCount > 0 {
		elapsed := time.Since(startTime).Seconds()
		avgSpeed := float64(totalSize) / elapsed / (1024 * 1024)
		fmt.Printf("\n迁移完成. 总文件数: %d, 已迁移文件数: %d, 平均速度: %.2f MB/s\n", fileCount, migratedObjs, avgSpeed)

	}

	return nil
}

// fromLocalWorker 处理从本地到对象存储的迁移工作
func (m *Migrator) fromLocalWorker(ctx context.Context, workerID int, fileCh <-chan string) int {
	migratedObjs := 0

	m.logger.WithField("worker_id", workerID).Info("启动本地到对象存储迁移进程.")
	for {
		select {
		case filePath, ok := <-fileCh:
			if !ok { // 通道关闭，没有其他文件了
				m.logger.WithField("worker_id", workerID).Info("本地到对象存储迁移进程结束.")
				return migratedObjs
			}
			migratedObjs += m.migrateFileToS3(ctx, filePath)
		case <-ctx.Done():
			m.logger.WithField("worker_id", workerID).Warn("本地到对象存储迁移进程中止.")
			return migratedObjs
		}
	}
}

// migrateFileToS3 将单个本地文件迁移到对象存储
func (m *Migrator) migrateFileToS3(ctx context.Context, objKey string) int {

	logFields := logrus.Fields{"file_path": objKey}
	m.logger.WithFields(logFields).Debug("处理文件")

	// 构建完整的本地文件路径
	localFilePath := filepath.Join(m.localPath, objKey)
	remoteObjKey := filepath.Join(m.objPath, objKey)

	// 获取文件信息
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("获取文件信息失败")
		_ = RecordMigration(m.db, MigrationRecord{Path: objKey, Status: "FAILED"})
		return 0

	}

	// 检查迁移状态
	record, found, err := GetMigrationStatus(m.db, objKey)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Warn("数据库中对象状态未知，强制迁移.")
	} else if found && record.Status == "COMPLETED" && record.SourceSize == fileInfo.Size() {
		// 注意：本地文件没有ETag，只能通过大小和修改时间比较
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_size": fileInfo.Size(),
			"status":      "SKIPPED",
			"reason":      "Already migrated and matches source",
		}).Info("当前文件已迁移，跳过当前文件")

		_ = RecordMigration(m.db, MigrationRecord{
			Path:            objKey,
			SourceSize:      fileInfo.Size(),
			DestinationETag: record.DestinationETag,
			DestinationSize: record.DestinationSize,
			Status:          "SKIPPED",
		})
		return 0

	} else if found && record.Status == "COMPLETED" && record.SourceSize != fileInfo.Size() {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"old_source_size": record.SourceSize,
			"new_source_size": fileInfo.Size(),
			"reason":          "Source file changed, re-migrating",
		}).Info("当前文件已迁移，但是源文件已修改. 尝试再次迁移.")
	} else if found && (record.Status == "FAILED" || record.Status == "SKIPPED") {
		m.logger.WithFields(logFields).WithField("previous_status", record.Status).Info("当前文件尚未完成迁移. 尝试再次迁移.")
	}

	if !m.dryrun {
		// 打开文件
		file, err := os.Open(localFilePath)
		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("打开本地文件失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: objKey, SourceSize: fileInfo.Size(), Status: "FAILED"})
			return 0

		}
		defer file.Close()

		// 检测内容类型
		contentType := "application/octet-stream" // 默认二进制类型
		buffer := make([]byte, 512)
		_, err = file.Read(buffer)
		if err == nil {
			contentType = http.DetectContentType(buffer)
			// 重置文件指针到开头
			_, err = file.Seek(0, 0)
			if err != nil {
				m.logger.WithFields(logFields).WithError(err).Error("重置文件指针失败")
				_ = RecordMigration(m.db, MigrationRecord{Path: objKey, SourceSize: fileInfo.Size(), Status: "FAILED"})
				return 0

			}
		}
		m.logger.WithFields(logFields).Info("处理文件" + objKey + "  to " + remoteObjKey)
		// 上传到对象存储
		uploadInfo, err := m.destClient.PutObject(ctx, m.destBucket, remoteObjKey, file, fileInfo.Size(), minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err != nil {
			m.logger.WithFields(logFields).WithError(err).Error("文件上传到目标桶失败")
			_ = RecordMigration(m.db, MigrationRecord{Path: objKey, SourceSize: fileInfo.Size(), Status: "FAILED"})
			return 0

		}

		if uploadInfo.Size != fileInfo.Size() {
			m.logger.WithFields(logFields).WithFields(logrus.Fields{
				"source_size":      fileInfo.Size(),
				"destination_size": uploadInfo.Size,
				"issue":            "Size mismatch",
			}).Error("迁移验证失败: 文件大小不对")
			_ = RecordMigration(m.db, MigrationRecord{
				Path:            objKey,
				SourceSize:      fileInfo.Size(),
				DestinationETag: uploadInfo.ETag,
				DestinationSize: uploadInfo.Size,
				Status:          "FAILED",
			})
			return 0

		}

		// 记录迁移成功
		err = RecordMigration(m.db, MigrationRecord{
			Path:            objKey,
			SourceSize:      fileInfo.Size(),
			DestinationETag: uploadInfo.ETag,
			DestinationSize: uploadInfo.Size,
			Status:          "COMPLETED",
		})
	} else {
		m.logger.WithFields(logFields).WithField("file", objKey).Info("dryrun, 不实际上传文件.")
	}

	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("数据库记录迁移成功状态失败")
		// 这是一个关键错误，但迁移本身是成功的。记录并继续。
	} else {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"size": fileInfo.Size(),
		}).Info("当前文件迁移到对象存储完成")
	}
	return 1

}

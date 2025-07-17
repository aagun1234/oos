package main

import (
	"context"
	"database/sql"
	"fmt"
	//"io" // Used for io.Reader for PutObject
	"strings"
	"sync"
	//"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus" // Import logrus
)

// Migrator holds the S3 clients, database connection, and migration settings.
type Migrator struct {
	sourceClient *minio.Client
	destClient   *minio.Client
	sourceBucket string
	destBucket   string
	db           *sql.DB
	concurrency  int
	prefix       string
	logger       *logrus.Logger // Add logger field
}

// NewMigrator initializes and returns a new Migrator.
func NewMigrator(sourceCfg, destCfg S3Config, db *sql.DB, concurrency int, logger *logrus.Logger) (*Migrator, error) {
	srcClient, err := minio.New(sourceCfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(sourceCfg.AccessKeyID, sourceCfg.SecretAccessKey, ""),
		Secure: sourceCfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create source S3 client: %w", err)
	}

	destClient, err := minio.New(destCfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(destCfg.AccessKeyID, destCfg.SecretAccessKey, ""),
		Secure: destCfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create destination S3 client: %w", err)
	}

	return &Migrator{
		sourceClient: srcClient,
		destClient:   destClient,
		sourceBucket: sourceCfg.Bucket,
		destBucket:   destCfg.Bucket,
		db:           db,
		prefix:       sourceCfg.Prefix,
		concurrency:  concurrency,
		logger:       logger, // Assign logger
	}, nil
}

// StartMigration starts the migration process.
func (m *Migrator) StartMigration(ctx context.Context) error {
	m.logger.WithFields(logrus.Fields{
		"source_endpoint": m.sourceClient.EndpointURL().Host,
		"source_bucket":   m.sourceBucket,
		"dest_endpoint":   m.destClient.EndpointURL().Host,
		"dest_bucket":     m.destBucket,
		"concurrency":     m.concurrency,
		"prefix":          m.prefix,
	}).Info("Starting migration process")
	objPrefix:=m.prefix

	// Check if destination bucket exists, create if not
	exists, err := m.destClient.BucketExists(ctx, m.destBucket)
	if err != nil {
		return fmt.Errorf("failed to check destination bucket existence: %w", err)
	}
	if !exists {
		m.logger.WithField("bucket", m.destBucket).Info("Destination bucket does not exist. Creating it...")
		err = m.destClient.MakeBucket(ctx, m.destBucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create destination bucket '%s': %w", m.destBucket, err)
		}
		m.logger.WithField("bucket", m.destBucket).Info("Destination bucket created successfully.")
	}

	objectCh := make(chan minio.ObjectInfo, m.concurrency*2) // Buffer for objects to process
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < m.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			m.worker(ctx, workerID, objectCh)
		}(i)
	}

	// List objects from source bucket and send to channel
	go func() {
		defer close(objectCh)
		listCtx, cancel := context.WithCancel(ctx)
		defer cancel() // Ensure cancellation if listing stops early

		opts := minio.ListObjectsOptions{
			Prefix:    objPrefix,
			Recursive: true, // List all objects recursively
		}

		objectCount := 0
		for object := range m.sourceClient.ListObjects(listCtx, m.sourceBucket, opts) {
			if object.Err != nil {
				m.logger.WithError(object.Err).Warn("Error listing object")
				continue
			}
			
			
			select {
			case objectCh <- object:
				objectCount++
			case <-ctx.Done(): // Check if main context cancelled
				m.logger.WithField("objects_listed", objectCount).Info("Migration cancelled during object listing.")
				return
			}
		}
		m.logger.WithField("total_objects_found", objectCount).Info("Finished listing objects.")
	}()

	wg.Wait() // Wait for all workers to finish

	m.logger.Info("Migration process completed.")

	// Report summary
	completed, _ := CountMigratedObjects(m.db, "COMPLETED")
	failed, _ := CountMigratedObjects(m.db, "FAILED")
	skipped, _ := CountMigratedObjects(m.db, "SKIPPED")
	m.logger.WithFields(logrus.Fields{
		"completed": completed,
		"failed":    failed,
		"skipped":   skipped,
	}).Info("Migration Summary")

	return nil
}

// worker processes objects from the object channel.
func (m *Migrator) worker(ctx context.Context, workerID int, objectCh <-chan minio.ObjectInfo) {
	m.logger.WithField("worker_id", workerID).Info("Worker started.")
	for {
		select {
		case object, ok := <-objectCh:
			if !ok { // Channel closed, no more objects
				m.logger.WithField("worker_id", workerID).Info("Worker finished.")
				return
			}
			m.migrateObject(ctx, object)
		case <-ctx.Done():
			m.logger.WithField("worker_id", workerID).Warn("Worker received cancellation signal. Exiting.")
			return
		}
	}
}

// migrateObject handles the migration of a single object.
func (m *Migrator) migrateObject(ctx context.Context, obj minio.ObjectInfo) {
	logFields := logrus.Fields{"object_path": obj.Key}
	m.logger.WithFields(logFields).Debug("Processing object")

	// 1. Check DB for existing migration record
	record, found, err := GetMigrationStatus(m.db, obj.Key)
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Warn("Error checking DB for object status. Attempting migration anyway.")
		// Don't return, try to migrate and record later
	} else if found && record.Status == "COMPLETED" && record.SourceETag == obj.ETag && record.SourceSize == obj.Size {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_etag":  obj.ETag,
			"source_size":  obj.Size,
			"status":       "SKIPPED",
			"reason":       "Already migrated and matches source",
		}).Info("Skipping object")
		// Optional: update timestamp in DB if we want to mark it as "recently seen"
		_ = RecordMigration(m.db, MigrationRecord{
			Path:            obj.Key,
			SourceETag:      obj.ETag,
			SourceSize:      obj.Size,
			DestinationETag: record.DestinationETag, // Keep old destination info
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
		}).Info("Object previously completed but source changed. Re-migrating.")
		// Proceed with migration
	} else if found && (record.Status == "FAILED" || record.Status == "SKIPPED") {
		m.logger.WithFields(logFields).WithField("previous_status", record.Status).Info("Object in DB with non-completed status. Re-attempting migration.")
		// Proceed with migration
	}

	// 2. Download from source
	sourceObject, err := m.sourceClient.GetObject(ctx, m.sourceBucket, obj.Key, minio.GetObjectOptions{})
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("Error downloading object from source")
		_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
		return
	}
	defer sourceObject.Close()

	// 3. Upload to destination
	uploadInfo, err := m.destClient.PutObject(ctx, m.destBucket, obj.Key, sourceObject, obj.Size, minio.PutObjectOptions{
		ContentType:  obj.ContentType,  // Preserve content type
		UserMetadata: obj.UserMetadata, // Preserve custom metadata if any
	})
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("Error uploading object to destination")
		_ = RecordMigration(m.db, MigrationRecord{Path: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, Status: "FAILED"})
		return
	}

	// 4. Verify file size and ETag
	if uploadInfo.Size != obj.Size {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_size":      obj.Size,
			"destination_size": uploadInfo.Size,
			"issue":            "Size mismatch",
		}).Error("Migration verification failed: Size mismatch")
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

	// Note: S3 ETag behavior can be tricky with multipart uploads (they aren't simple MD5s)
	// For simple files, ETag usually matches if content is identical.
	// For multipart uploads, S3 appends "-PARTS" to the MD5 hash.
	// We'll compare ETags only if they are both non-empty and not multipart (simplified check).
	// A more robust check might involve comparing checksums if available, or just relying on size.
	sourceETag := strings.Trim(obj.ETag, `"`)
	destETag := strings.Trim(uploadInfo.ETag, `"`)
	if sourceETag != "" && destETag != "" && sourceETag != destETag &&
		!strings.Contains(sourceETag, "-") && !strings.Contains(destETag, "-") {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"source_etag": sourceETag,
			"dest_etag":   destETag,
			"issue":       "ETag mismatch",
			"note":        "May be due to multipart upload",
		}).Warn("Migration verification warning: ETag mismatch")
	}

	// 5. Record success in DB
	err = RecordMigration(m.db, MigrationRecord{
		Path:            obj.Key,
		SourceETag:      obj.ETag,
		SourceSize:      obj.Size,
		DestinationETag: uploadInfo.ETag,
		DestinationSize: uploadInfo.Size,
		Status:          "COMPLETED",
	})
	if err != nil {
		m.logger.WithFields(logFields).WithError(err).Error("Error recording successful migration to DB")
		// This is a critical error, but migration itself was successful. Log and continue.
	} else {
		m.logger.WithFields(logFields).WithFields(logrus.Fields{
			"size": obj.Size,
			"etag": obj.ETag,
		}).Info("Successfully migrated object")
	}
}
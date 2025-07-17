package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type MigrationRecord struct {
	Path           string
	SourceETag     string
	SourceSize     int64
	DestinationETag string
	DestinationSize int64
	Status         string // "COMPLETED", "FAILED", "SKIPPED"
	MigratedAt     time.Time
}

func OpenDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("[数据库] 打开数据库失败: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite写操作单连接
	schema := `
	CREATE TABLE IF NOT EXISTS migrations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		object_path TEXT NOT NULL UNIQUE,
		source_etag TEXT,
		source_size INTEGER,
		destination_etag TEXT,
		destination_size INTEGER,
		status TEXT NOT NULL,
		migrated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(schema)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("[数据库] 创建表失败: %w", err)
	}
	return db, nil
}

func RecordMigration(db *sql.DB, record MigrationRecord) error {
	stmt, err := db.Prepare(`
		INSERT OR REPLACE INTO migrations (
			object_path, source_etag, source_size, destination_etag, destination_size, status, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return fmt.Errorf("[数据库] 数据库错误: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		record.Path,
		record.SourceETag,
		record.SourceSize,
		record.DestinationETag,
		record.DestinationSize,
		record.Status,
		time.Now().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("[数据库] 执行失败: %w", err)
	}
	return nil
}


func GetMigrationStatus(db *sql.DB, objectPath string) (MigrationRecord, bool, error) {
	row := db.QueryRow(`
		SELECT object_path, source_etag, source_size, destination_etag, destination_size, status, migrated_at
		FROM migrations WHERE object_path = ?;
	`, objectPath)

	var record MigrationRecord
	var migratedAtStr string
	err := row.Scan(
		&record.Path,
		&record.SourceETag,
		&record.SourceSize,
		&record.DestinationETag,
		&record.DestinationSize,
		&record.Status,
		&migratedAtStr,
	)
	if err == sql.ErrNoRows {
		return MigrationRecord{}, false, nil 
	}
	if err != nil {
		return MigrationRecord{}, false, fmt.Errorf("[数据库] 查询迁移信息失败: %w", err)
	}

	record.MigratedAt, err = time.Parse(time.RFC3339, migratedAtStr)
	if err != nil {
		log.Printf("[数据库] 时间戳异常 %s: %v", objectPath, err)
	}

	return record, true, nil
}


func CountMigratedObjects(db *sql.DB, status string) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM migrations WHERE status = ?", status).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("[数据库] 获取对象数量失败: %w", err)
	}
	return count, nil
}
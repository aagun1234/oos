package main

import (
	"database/sql"
	"fmt"
	"log" // Using standard log for DB-internal simple warnings for now
	"time"

	_ "github.com/mattn/go-sqlite3" // Import for its side effects (driver registration)
)

// MigrationRecord represents a single file migration entry in the database.
type MigrationRecord struct {
	Path           string
	SourceETag     string
	SourceSize     int64
	DestinationETag string
	DestinationSize int64
	Status         string // "COMPLETED", "FAILED", "SKIPPED"
	MigratedAt     time.Time
}

// OpenDB opens a SQLite database connection.
func OpenDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite works best with a single connection for writes

	// Create table if it doesn't exist
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
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return db, nil
}

// RecordMigration saves or updates the migration status for an object.
func RecordMigration(db *sql.DB, record MigrationRecord) error {
	stmt, err := db.Prepare(`
		INSERT OR REPLACE INTO migrations (
			object_path, source_etag, source_size, destination_etag, destination_size, status, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
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
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// GetMigrationStatus retrieves the migration status for a given object path.
// Returns (record, true, nil) if found, (empty record, false, nil) if not found,
// or (empty record, false, error) on error.
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
		return MigrationRecord{}, false, nil // Not found
	}
	if err != nil {
		return MigrationRecord{}, false, fmt.Errorf("failed to query migration status: %w", err)
	}

	record.MigratedAt, err = time.Parse(time.RFC3339, migratedAtStr)
	if err != nil {
		// This is a minor issue, log a warning but don't fail the lookup
		log.Printf("Warning: Could not parse timestamp for %s: %v", objectPath, err)
	}

	return record, true, nil
}

// CountMigratedObjects counts the number of objects with a given status.
func CountMigratedObjects(db *sql.DB, status string) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM migrations WHERE status = ?", status).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count objects: %w", err)
	}
	return count, nil
}
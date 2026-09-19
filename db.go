// Package gofailedjobs retries failed queue jobs: load them from MySQL/Postgres
// and republish payloads via GORM or sqlx storage backends.
package gofailedjobs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// MySQL driver for sqlx.Connect("mysql", ...).
	_ "github.com/go-sql-driver/mysql"
	// PostgreSQL driver for sqlx.Connect("pgx", ...).
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Driver is the database access library selected in Config.
type Driver string

// Dialect is the DBMS used to open a connection.
type Dialect string

const (
	// DriverGorm is [gorm.io/gorm].
	DriverGorm Driver = "gorm"
	// DriverSqlx is [github.com/jmoiron/sqlx].
	DriverSqlx Driver = "sqlx"

	// DialectMySQL is MySQL / MariaDB.
	DialectMySQL Dialect = "mysql"
	// DialectPostgres is PostgreSQL.
	DialectPostgres Dialect = "postgres"

	sqlDriverMySQL = "mysql"
	sqlDriverPgx   = "pgx"

	defaultDBPingTimeout = 5 * time.Second
)

// Config selects the DB library, dialect, and connection pool settings.
// An empty Driver is treated as DriverGorm. Dialect is required: mysql or postgres.
type Config struct {
	Driver            Driver
	Dialect           Dialect
	FormatDSN         string
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration
	// DBPingTimeout is the Connect/Ping timeout. Zero means 5s.
	DBPingTimeout time.Duration
}

func (c Config) driver() Driver {
	d := Driver(strings.ToLower(strings.TrimSpace(string(c.Driver))))
	if d == "" {
		return DriverGorm
	}

	return d
}

func (c Config) dialect() Dialect {
	name := Dialect(strings.ToLower(strings.TrimSpace(string(c.Dialect))))
	switch name {
	case DialectPostgres, "postgresql", "pg":
		return DialectPostgres
	case DialectMySQL, "mariadb":
		return DialectMySQL
	default:
		return name
	}
}

func (c Config) pingTimeout() time.Duration {
	if c.DBPingTimeout > 0 {
		return c.DBPingTimeout
	}

	return defaultDBPingTimeout
}

func dialectError(d Dialect) error {
	if strings.TrimSpace(string(d)) == "" {
		return fmt.Errorf("%w: empty value (expected %q or %q)", ErrUnsupportedDialect, DialectMySQL, DialectPostgres)
	}

	return fmt.Errorf("%w: %q (expected %q or %q)", ErrUnsupportedDialect, d, DialectMySQL, DialectPostgres)
}

func sqlxDriverName(cfg Config) (string, error) {
	switch cfg.dialect() {
	case DialectMySQL:
		return sqlDriverMySQL, nil
	case DialectPostgres:
		return sqlDriverPgx, nil
	default:
		return "", dialectError(cfg.Dialect)
	}
}

// ConnectGorm opens a GORM connection (MySQL or PostgreSQL) and configures the pool.
func ConnectGorm(cfg Config) (*gorm.DB, error) {
	var dialector gorm.Dialector

	switch cfg.dialect() {
	case DialectMySQL:
		dialector = mysql.Open(cfg.FormatDSN)
	case DialectPostgres:
		dialector = postgres.Open(cfg.FormatDSN)
	default:
		return nil, dialectError(cfg.Dialect)
	}

	//nolint:exhaustruct,exhaustruct_v5 // GORM zero values are intentional
	gdb, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("opening gorm: %w", err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("getting sql.DB from gorm: %w", err)
	}

	err = applyPool(sqlDB, cfg)
	if err != nil {
		_ = sqlDB.Close()

		return nil, fmt.Errorf("configuring gorm pool: %w", err)
	}

	return gdb, nil
}

// ConnectSqlx opens a sqlx connection (MySQL or PostgreSQL) and configures the pool.
func ConnectSqlx(cfg Config) (*sqlx.DB, error) {
	driverName, err := sqlxDriverName(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.pingTimeout())
	defer cancel()

	sqlxDB, err := sqlx.ConnectContext(ctx, driverName, cfg.FormatDSN)
	if err != nil {
		return nil, fmt.Errorf("opening sqlx: %w", err)
	}

	err = applyPool(sqlxDB.DB, cfg)
	if err != nil {
		_ = sqlxDB.Close()

		return nil, fmt.Errorf("configuring sqlx pool: %w", err)
	}

	return sqlxDB, nil
}

func applyPool(sqlDB *sql.DB, cfg Config) error {
	sqlDB.SetMaxIdleConns(cfg.DBMaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.DBMaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.pingTimeout())
	defer cancel()

	err := sqlDB.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	return nil
}

func closeGorm(gdb *gorm.DB) error {
	sqlDB, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("getting sql.DB from gorm: %w", err)
	}

	err = sqlDB.Close()
	if err != nil {
		return fmt.Errorf("closing gorm connection: %w", err)
	}

	return nil
}

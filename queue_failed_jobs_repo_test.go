package gofailedjobs

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/amqp-adapter"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	_ "github.com/mattn/go-sqlite3"
)

func setupGormRepo(t *testing.T) *QueueFailedJobRepo {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.AutoMigrate(&QueueFailedJob{}))

	return NewGormQueueFailedJobRepo(db)
}

func setupSqlxRepo(t *testing.T) *QueueFailedJobRepo {
	t.Helper()

	db, err := sqlx.Connect("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	_, err = db.Exec(`CREATE TABLE queue_failed_jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		connection TEXT,
		queue TEXT,
		payload TEXT,
		exception TEXT,
		failed_at TEXT,
		maintenance_comment TEXT
	)`)
	require.NoError(t, err)

	return NewSqlxQueueFailedJobRepo(db)
}

func TestQueueFailedJobRepo(t *testing.T) {
	backends := []struct {
		name  string
		setup func(t *testing.T) *QueueFailedJobRepo
		want  Driver
	}{
		{name: "gorm", setup: setupGormRepo, want: DriverGorm},
		{name: "sqlx", setup: setupSqlxRepo, want: DriverSqlx},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			repo := backend.setup(t)
			assert.Equal(t, backend.want, repo.Driver())
			assert.NoError(t, repo.Close())

			testRepoGetByIDs(t, backend.setup)
			testRepoAdd(t, backend.setup)
			testRepoDelete(t, backend.setup)
			testRepoAddWithID(t, backend.setup)
			testRepoDeleteMissing(t, backend.setup)
			testRepoDeleteInvalidID(t, backend.setup)
			testRepoRetry(t, backend.setup)
		})
	}
}

func testRepoGetByIDs(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	now := time.Now().Format(failedAtLayout)
	jobs := []QueueFailedJob{
		{Connection: "test1", Queue: "queue1", Payload: "payload1", Exception: "error1", FailedAt: now},
		{Connection: "test2", Queue: "queue2", Payload: "payload2", Exception: "error2", FailedAt: now},
	}

	for _, job := range jobs {
		require.NoError(t, repo.Add(context.Background(), job))
	}

	tests := []struct {
		name string
		ids  []int
		want int
	}{
		{name: "existing rows", ids: []int{1, 2}, want: 2},
		{name: "missing row", ids: []int{999}, want: 0},
		{name: "empty id list", ids: []int{}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repo.GetByIDs(context.Background(), tt.ids)
			require.NoError(t, err)
			assert.Len(t, got, tt.want)
		})
	}
}

func testRepoAdd(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	now := time.Now().Format(failedAtLayout)
	job := QueueFailedJob{
		Connection: "test",
		Queue:      "queue",
		Payload:    "payload",
		Exception:  "error",
		FailedAt:   now,
	}

	require.NoError(t, repo.Add(context.Background(), job))

	saved, err := repo.GetByIDs(context.Background(), []int{1})
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, job.Connection, saved[0].Connection)
	assert.Equal(t, job.Queue, saved[0].Queue)
	assert.Equal(t, job.Payload, saved[0].Payload)
	assert.Equal(t, job.Exception, saved[0].Exception)
	assert.Equal(t, job.FailedAt, saved[0].FailedAt)
}

func testRepoAddWithID(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	now := time.Now().Format(failedAtLayout)
	job := QueueFailedJob{
		ID:         42,
		Connection: "test",
		Queue:      "queue",
		Payload:    "payload",
		Exception:  "error",
		FailedAt:   now,
	}

	require.NoError(t, repo.Add(context.Background(), job))

	saved, err := repo.GetByIDs(context.Background(), []int{42})
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, 42, saved[0].ID)
}

func testRepoDelete(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	now := time.Now().Format(failedAtLayout)
	job := QueueFailedJob{
		Connection: "test",
		Queue:      "queue",
		Payload:    "payload",
		Exception:  "error",
		FailedAt:   now,
	}

	require.NoError(t, repo.Add(context.Background(), job))

	saved, err := repo.GetByIDs(context.Background(), []int{1})
	require.NoError(t, err)
	require.Len(t, saved, 1)

	require.NoError(t, repo.Delete(context.Background(), saved[0]))

	got, err := repo.GetByIDs(context.Background(), []int{1})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func testRepoDeleteMissing(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	err := repo.Delete(context.Background(), QueueFailedJob{ID: 999})
	require.ErrorIs(t, err, ErrJobNotDeleted)
}

func testRepoDeleteInvalidID(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	err := repo.Delete(context.Background(), QueueFailedJob{ID: 0})
	require.ErrorIs(t, err, ErrInvalidJobID)
}

func testRepoRetry(t *testing.T, setup func(t *testing.T) *QueueFailedJobRepo) {
	t.Helper()

	repo := setup(t)
	now := time.Now().Format(failedAtLayout)
	require.NoError(t, repo.Add(context.Background(), QueueFailedJob{
		Connection: "test",
		Queue:      "queue",
		Payload:    "payload",
		Exception:  "error",
		FailedAt:   now,
	}))

	srv := NewQueueFailedJobSrv(repo, new(MockLogger)).SetConnection("test")
	q := &mockQueue{}
	require.NoError(t, srv.Retry(context.Background(), []int{1}, q))

	got, err := repo.GetByIDs(context.Background(), []int{1})
	require.NoError(t, err)
	assert.Empty(t, got)
	require.Len(t, q.published, 1)
	assert.Equal(t, mq.QueueName("queue"), q.published[0].queue)
	assert.Equal(t, "payload", string(q.published[0].msg.Body))
}

func TestNewQueueFailedJobRepo_unsupportedDriver(t *testing.T) {
	repo, err := NewQueueFailedJobRepo(Config{
		Driver:            "foo",
		Dialect:           DialectMySQL,
		FormatDSN:         "user:pass@tcp(127.0.0.1:3306)/db",
		DBMaxOpenConns:    1,
		DBMaxIdleConns:    1,
		DBConnMaxLifetime: time.Second,
	})
	require.ErrorIs(t, err, ErrUnsupportedDriver)
	assert.Nil(t, repo)
}

func TestNewQueueFailedJobRepo_unsupportedDialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
	}{
		{name: "empty value", dialect: ""},
		{name: "unknown", dialect: "oracle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo, err := NewQueueFailedJobRepo(Config{
				Driver:            DriverGorm,
				Dialect:           tt.dialect,
				FormatDSN:         "user:pass@tcp(127.0.0.1:3306)/db",
				DBMaxOpenConns:    1,
				DBMaxIdleConns:    1,
				DBConnMaxLifetime: time.Second,
			})
			require.ErrorIs(t, err, ErrUnsupportedDialect)
			assert.Nil(t, repo)
		})
	}
}

func TestNewQueueFailedJobRepo_invalidDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		driver  Driver
		dialect Dialect
		dsn     string
	}{
		{name: "gorm mysql", driver: DriverGorm, dialect: DialectMySQL, dsn: "invalid://dsn"},
		{name: "sqlx mysql", driver: DriverSqlx, dialect: DialectMySQL, dsn: "invalid://dsn"},
		{name: "gorm postgres", driver: DriverGorm, dialect: DialectPostgres, dsn: "invalid://dsn"},
		{name: "sqlx postgres", driver: DriverSqlx, dialect: DialectPostgres, dsn: "invalid://dsn"},
		{name: "default gorm mysql", driver: "", dialect: DialectMySQL, dsn: "invalid://dsn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo, err := NewQueueFailedJobRepo(Config{
				Driver:            tt.driver,
				Dialect:           tt.dialect,
				FormatDSN:         tt.dsn,
				DBMaxOpenConns:    1,
				DBMaxIdleConns:    1,
				DBConnMaxLifetime: time.Second,
			})
			require.Error(t, err)
			assert.Nil(t, repo)
		})
	}
}

func TestConfig_driver(t *testing.T) {
	t.Parallel()

	assert.Equal(t, DriverGorm, Config{}.driver())
	assert.Equal(t, DriverGorm, Config{Driver: "GORM"}.driver())
	assert.Equal(t, DriverSqlx, Config{Driver: " SQLX "}.driver())
	assert.Equal(t, Driver("foo"), Config{Driver: "foo"}.driver())
	assert.Equal(t, Dialect(""), Config{}.dialect())
	assert.Equal(t, DialectMySQL, Config{Dialect: "MYSQL"}.dialect())
	assert.Equal(t, DialectMySQL, Config{Dialect: " mariadb "}.dialect())
	assert.Equal(t, DialectPostgres, Config{Dialect: "PostgreSQL"}.dialect())
	assert.Equal(t, DialectPostgres, Config{Dialect: "pg"}.dialect())
	assert.Equal(t, Dialect("oracle"), Config{Dialect: "oracle"}.dialect())
	assert.Equal(t, defaultDBPingTimeout, Config{}.pingTimeout())
	assert.Equal(t, 2*time.Second, Config{DBPingTimeout: 2 * time.Second}.pingTimeout())
}

func TestSupportsRowLock(t *testing.T) {
	t.Parallel()

	assert.False(t, supportsRowLock(driverSQLite))
	assert.False(t, supportsRowLock(driverSQLite3))
	assert.True(t, supportsRowLock("mysql"))
	assert.True(t, supportsRowLock("postgres"))
	assert.True(t, supportsRowLock("pgx"))
}

type captureSqlxQueryer struct {
	lastQuery string
	lastArgs  []any
}

func (c *captureSqlxQueryer) SelectContext(_ context.Context, dest any, query string, args ...any) error {
	c.lastQuery = query
	c.lastArgs = args
	ptr, ok := dest.(*[]QueueFailedJob)
	if ok {
		*ptr = []QueueFailedJob{}
	}

	return nil
}

func (c *captureSqlxQueryer) NamedExecContext(context.Context, string, any) (sql.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *captureSqlxQueryer) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *captureSqlxQueryer) Rebind(query string) string {
	return query
}

func TestSqlxRepo_GetByIDs_forUpdate(t *testing.T) {
	t.Parallel()

	cap := &captureSqlxQueryer{}
	repo := &sqlxRepo{db: nil, ext: cap, driverName: "pgx", forUpdate: true}

	_, err := repo.GetByIDs(context.Background(), []int{1, 2})
	require.NoError(t, err)
	assert.Contains(t, cap.lastQuery, "FOR UPDATE")
	assert.Contains(t, cap.lastQuery, "queue_failed_jobs")
}

func TestSqlxRepo_GetByIDs_skipsForUpdateOnSQLite(t *testing.T) {
	t.Parallel()

	cap := &captureSqlxQueryer{}
	repo := &sqlxRepo{db: nil, ext: cap, driverName: driverSQLite3, forUpdate: true}

	_, err := repo.GetByIDs(context.Background(), []int{1})
	require.NoError(t, err)
	assert.NotContains(t, cap.lastQuery, "FOR UPDATE")
}

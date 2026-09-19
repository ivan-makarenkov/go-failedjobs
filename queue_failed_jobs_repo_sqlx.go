package gofailedjobs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const (
	selectFailedJobsByIDs = `SELECT id, connection, queue, payload, exception, failed_at, maintenance_comment
FROM queue_failed_jobs WHERE id IN (?)`
	insertFailedJob = `INSERT INTO queue_failed_jobs
(connection, queue, payload, exception, failed_at, maintenance_comment)
VALUES (:connection, :queue, :payload, :exception, :failed_at, :maintenance_comment)`
	insertFailedJobWithID = `INSERT INTO queue_failed_jobs
(id, connection, queue, payload, exception, failed_at, maintenance_comment)
VALUES (:id, :connection, :queue, :payload, :exception, :failed_at, :maintenance_comment)`
	deleteFailedJob = `DELETE FROM queue_failed_jobs WHERE id = ?`
)

type sqlxQueryer interface {
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Rebind(query string) string
}

type sqlxRepo struct {
	db         *sqlx.DB
	ext        sqlxQueryer
	driverName string
	forUpdate  bool
}

func newSqlxRepo(db *sqlx.DB) *sqlxRepo {
	return &sqlxRepo{db: db, ext: db, driverName: db.DriverName(), forUpdate: false}
}

func (r *sqlxRepo) InTx(
	ctx context.Context,
	txFn func(context.Context, QueueFailedJobRepoInterface) error,
) error {
	if r.db == nil {
		return txFn(ctx, r)
	}

	txn, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning queue_failed_jobs transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = txn.Rollback()
		}
	}()

	err = txFn(ctx, &sqlxRepo{db: nil, ext: txn, forUpdate: true, driverName: r.driverName})
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("committing queue_failed_jobs transaction: %w", err)
	}

	committed = true

	return nil
}

func (r *sqlxRepo) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	if len(ids) == 0 {
		return []QueueFailedJob{}, nil
	}

	query, args, err := sqlx.In(selectFailedJobsByIDs, ids)
	if err != nil {
		return nil, fmt.Errorf("preparing queue_failed_jobs IN query: %w", err)
	}

	if r.forUpdate && supportsRowLock(r.driverName) {
		query += " FOR UPDATE"
	}

	query = r.ext.Rebind(query)

	var queueFailedJobs []QueueFailedJob

	err = r.ext.SelectContext(ctx, &queueFailedJobs, query, args...)
	if err != nil {
		return nil, fmt.Errorf("selecting queue_failed_jobs: %w", err)
	}

	if queueFailedJobs == nil {
		return []QueueFailedJob{}, nil
	}

	return queueFailedJobs, nil
}

func (r *sqlxRepo) Add(ctx context.Context, queueFailedJob QueueFailedJob) error {
	query := insertFailedJob
	if queueFailedJob.ID > 0 {
		query = insertFailedJobWithID
	}

	_, err := r.ext.NamedExecContext(ctx, query, queueFailedJob)
	if err != nil {
		return fmt.Errorf("inserting queue_failed_jobs: %w", err)
	}

	return nil
}

func (r *sqlxRepo) Delete(ctx context.Context, queueFailedJob QueueFailedJob) error {
	if queueFailedJob.ID <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidJobID, queueFailedJob.ID)
	}

	res, err := r.ext.ExecContext(ctx, r.ext.Rebind(deleteFailedJob), queueFailedJob.ID)
	if err != nil {
		return fmt.Errorf("deleting queue_failed_jobs: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleting queue_failed_jobs: %w", err)
	}

	if n == 0 {
		return fmt.Errorf("%w: id=%d", ErrJobNotDeleted, queueFailedJob.ID)
	}

	return nil
}

var _ QueueFailedJobRepoInterface = (*sqlxRepo)(nil)

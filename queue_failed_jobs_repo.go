package gofailedjobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gorm.io/gorm"
)

// QueueFailedJobRepo is a failed-jobs repository. The concrete DB library
// is selected via Config.Driver or the NewGorm/NewSqlx constructors.
type QueueFailedJobRepo struct {
	impl   QueueFailedJobRepoInterface
	driver Driver
	close  func() error
}

// NewQueueFailedJobRepo opens a connection with the selected library and returns a repository.
// The connection is owned by the repository and closed via Close.
func NewQueueFailedJobRepo(cfg Config) (*QueueFailedJobRepo, error) {
	switch cfg.driver() {
	case DriverGorm:
		gdb, err := ConnectGorm(cfg)
		if err != nil {
			return nil, err
		}

		return newOwnedRepo(newGormRepo(gdb), DriverGorm, func() error {
			return closeGorm(gdb)
		}), nil
	case DriverSqlx:
		sdb, err := ConnectSqlx(cfg)
		if err != nil {
			return nil, err
		}

		return newOwnedRepo(newSqlxRepo(sdb), DriverSqlx, sdb.Close), nil
	default:
		return nil, fmt.Errorf("%w: %q (expected %q or %q)", ErrUnsupportedDriver, cfg.Driver, DriverGorm, DriverSqlx)
	}
}

// NewGormQueueFailedJobRepo wraps an already open GORM connection.
// The caller owns and must close the connection.
func NewGormQueueFailedJobRepo(db *gorm.DB) *QueueFailedJobRepo {
	return &QueueFailedJobRepo{
		impl:   newGormRepo(db),
		driver: DriverGorm,
		close:  nil,
	}
}

// NewSqlxQueueFailedJobRepo wraps an already open sqlx connection.
// The caller owns and must close the connection.
func NewSqlxQueueFailedJobRepo(db *sqlx.DB) *QueueFailedJobRepo {
	return &QueueFailedJobRepo{
		impl:   newSqlxRepo(db),
		driver: DriverSqlx,
		close:  nil,
	}
}

func newOwnedRepo(impl QueueFailedJobRepoInterface, driver Driver, closer func() error) *QueueFailedJobRepo {
	return &QueueFailedJobRepo{
		impl:   impl,
		driver: driver,
		close:  closer,
	}
}

// Driver returns the DB library used by the repository.
func (r *QueueFailedJobRepo) Driver() Driver {
	return r.driver
}

// Close closes the connection if the repository was created via NewQueueFailedJobRepo.
func (r *QueueFailedJobRepo) Close() error {
	if r.close == nil {
		return nil
	}

	err := r.close()
	if err != nil {
		return fmt.Errorf("closing database connection: %w", err)
	}

	r.close = nil

	return nil
}

// GetByIDs returns failed jobs by identifiers.
func (r *QueueFailedJobRepo) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	jobs, err := r.impl.GetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("failed jobs repository: %w", err)
	}

	return jobs, nil
}

// Add stores a failed job.
func (r *QueueFailedJobRepo) Add(ctx context.Context, queueFailedJob QueueFailedJob) error {
	err := r.impl.Add(ctx, queueFailedJob)
	if err != nil {
		return fmt.Errorf("failed jobs repository: %w", err)
	}

	return nil
}

// Delete removes a failed job.
func (r *QueueFailedJobRepo) Delete(ctx context.Context, queueFailedJob QueueFailedJob) error {
	err := r.impl.Delete(ctx, queueFailedJob)
	if err != nil {
		return fmt.Errorf("failed jobs repository: %w", err)
	}

	return nil
}

// InTx runs fn inside a transaction of the underlying DB library.
func (r *QueueFailedJobRepo) InTx(
	ctx context.Context,
	txFn func(ctx context.Context, repo QueueFailedJobRepoInterface) error,
) error {
	err := r.impl.InTx(ctx, txFn)
	if err != nil {
		return fmt.Errorf("failed jobs repository: %w", err)
	}

	return nil
}

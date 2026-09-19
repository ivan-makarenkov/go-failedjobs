package gofailedjobs

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type gormRepo struct {
	db        *gorm.DB
	forUpdate bool
}

func newGormRepo(db *gorm.DB) *gormRepo {
	return &gormRepo{db: db, forUpdate: false}
}

func (r *gormRepo) InTx(
	ctx context.Context,
	txFn func(context.Context, QueueFailedJobRepoInterface) error,
) error {
	err := r.db.WithContext(ctx).Transaction(func(txn *gorm.DB) error {
		return txFn(ctx, &gormRepo{db: txn, forUpdate: true})
	})
	if err != nil {
		return fmt.Errorf("queue_failed_jobs transaction: %w", err)
	}

	return nil
}

func (r *gormRepo) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	if len(ids) == 0 {
		return []QueueFailedJob{}, nil
	}

	var queueFailedJobs []QueueFailedJob

	q := r.db.WithContext(ctx)
	if r.forUpdate && supportsRowLock(r.db.Name()) {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"}) //nolint:exhaustruct,exhaustruct_v5 // Table/Options unused
	}

	err := q.Find(&queueFailedJobs, ids).Error
	if err != nil {
		return nil, fmt.Errorf("selecting queue_failed_jobs: %w", err)
	}

	if queueFailedJobs == nil {
		return []QueueFailedJob{}, nil
	}

	return queueFailedJobs, nil
}

func (r *gormRepo) Add(ctx context.Context, queueFailedJob QueueFailedJob) error {
	err := r.db.WithContext(ctx).Create(&queueFailedJob).Error
	if err != nil {
		return fmt.Errorf("inserting queue_failed_jobs: %w", err)
	}

	return nil
}

func (r *gormRepo) Delete(ctx context.Context, queueFailedJob QueueFailedJob) error {
	if queueFailedJob.ID <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidJobID, queueFailedJob.ID)
	}

	res := r.db.WithContext(ctx).Delete(&queueFailedJob)
	if res.Error != nil {
		return fmt.Errorf("deleting queue_failed_jobs: %w", res.Error)
	}

	if res.RowsAffected == 0 {
		return fmt.Errorf("%w: id=%d", ErrJobNotDeleted, queueFailedJob.ID)
	}

	return nil
}

var _ QueueFailedJobRepoInterface = (*gormRepo)(nil)

package gofailedjobs

import (
	"context"
	"fmt"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const (
	queueFailedJobsTable = "queue_failed_jobs"

	// RoutePath is the HTTP path for retrying failed jobs.
	RoutePath = "/retry-task"

	maxRetryIDs = 100

	driverSQLite  = "sqlite"
	driverSQLite3 = "sqlite3"
)

// QueueFailedJob is a row in the queue_failed_jobs table.
type QueueFailedJob struct {
	ID                 int    `db:"id"                  gorm:"column:id;primaryKey"`
	Connection         string `db:"connection"          gorm:"column:connection"`
	Queue              string `db:"queue"               gorm:"column:queue"`
	Payload            string `db:"payload"             gorm:"column:payload"`
	Exception          string `db:"exception"           gorm:"column:exception"`
	FailedAt           string `db:"failed_at"           gorm:"column:failed_at"`
	MaintenanceComment string `db:"maintenance_comment" gorm:"column:maintenance_comment"`
}

// TableName returns the GORM table name.
func (f *QueueFailedJob) TableName() string {
	return queueFailedJobsTable
}

// Publisher publishes a message to the broker. Compatible with [mq.Queue].
type Publisher interface {
	Publish(ctx context.Context, queueName mq.QueueName, msg mq.PublishMessage) error
}

// QueueFailedJobRepoInterface is DB-library-agnostic storage for failed jobs.
//
// InTx runs fn inside a transaction. GORM/sqlx implementations load rows with
// SELECT FOR UPDATE inside the transaction. If transactions are unavailable,
// it is acceptable to call fn with the repository itself.
type QueueFailedJobRepoInterface interface {
	GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error)
	Add(ctx context.Context, queueFailedJob QueueFailedJob) error
	Delete(ctx context.Context, queueFailedJob QueueFailedJob) error
	InTx(ctx context.Context, fn func(ctx context.Context, repo QueueFailedJobRepoInterface) error) error
}

// QueueFailedJobSrvInterface retries and records failed jobs.
type QueueFailedJobSrvInterface interface {
	GetFailedJobHandler() func(qName string, msg string, errMsg string) error
	GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error)
	Add(ctx context.Context, queueFailedJob QueueFailedJob) error
	Delete(ctx context.Context, queueFailedJob QueueFailedJob) error
	Retry(ctx context.Context, ids []int, queue Publisher) error
}

func uniqueIDs(ids []int) []int {
	if len(ids) <= 1 {
		return ids
	}

	seen := make(map[int]struct{}, len(ids))
	out := make([]int, 0, len(ids))

	for _, jobID := range ids {
		if _, ok := seen[jobID]; ok {
			continue
		}

		seen[jobID] = struct{}{}

		out = append(out, jobID)
	}

	return out
}

func missingIDs(requested []int, found []QueueFailedJob) []int {
	have := make(map[int]struct{}, len(found))
	for _, job := range found {
		have[job.ID] = struct{}{}
	}

	missing := make([]int, 0)

	for _, jobID := range requested {
		if _, ok := have[jobID]; !ok {
			missing = append(missing, jobID)
		}
	}

	return missing
}

func normalizeIDs(ids []int, limit int) ([]int, error) {
	ids = uniqueIDs(ids)
	if len(ids) == 0 {
		return nil, ErrEmptyIDList
	}

	for _, jobID := range ids {
		if jobID <= 0 {
			return nil, fmt.Errorf("%w: %d", ErrInvalidJobID, jobID)
		}
	}

	if limit > 0 && len(ids) > limit {
		return nil, fmt.Errorf("%w: %d (max %d)", ErrTooManyIDs, len(ids), limit)
	}

	return ids, nil
}

func supportsRowLock(driver string) bool {
	switch driver {
	case driverSQLite, driverSQLite3:
		return false
	default:
		// MySQL and PostgreSQL (postgres/pgx) support SELECT FOR UPDATE.
		return true
	}
}

var (
	_ QueueFailedJobRepoInterface = (*QueueFailedJobRepo)(nil)
	_ QueueFailedJobSrvInterface  = (*QueueFailedJobSrv)(nil)
)

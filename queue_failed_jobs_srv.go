package gofailedjobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
)

const (
	failedAtLayout             = "2006-01-02 15:04:05"
	defaultFailedJobConnection = "rabbitmq"
	defaultMaxRetryDuration    = 20 * time.Minute
	defaultDBWriteTimeout      = 5 * time.Second
	defaultMaxRetryTimeout     = 2 * time.Minute
	failedJobCorrIDPrefix      = "gofailedjobs:failed-job:"
)

// PublisherResolver selects a broker by the failed job connection field.
// When unset, Retry uses the publisher passed as an argument and rejects
// rows whose connection differs from SetConnection.
type PublisherResolver func(connection string) (Publisher, error)

// QueueFailedJobSrv records failed jobs and republishes them for retry.
type QueueFailedJobSrv struct {
	repo              QueueFailedJobRepoInterface
	lgr               Logger
	connection        string
	maxRetryDuration  time.Duration
	maxRetryIDs       int
	dbWriteTimeout    time.Duration
	maxRetryTimeout   time.Duration
	handlerStop       <-chan struct{}
	publisherResolver PublisherResolver
	close             func() error
}

// NewQueueFailedJobSrv creates a service over any supported DB repository.
// A nil logger is replaced with a no-op so GetFailedJobHandler does not panic.
func NewQueueFailedJobSrv(repo QueueFailedJobRepoInterface, lgr Logger) *QueueFailedJobSrv {
	if lgr == nil {
		lgr = nopLogger{}
	}

	return &QueueFailedJobSrv{
		repo:              repo,
		lgr:               lgr,
		connection:        defaultFailedJobConnection,
		maxRetryDuration:  defaultMaxRetryDuration,
		maxRetryIDs:       maxRetryIDs,
		dbWriteTimeout:    defaultDBWriteTimeout,
		maxRetryTimeout:   defaultMaxRetryTimeout,
		handlerStop:       nil,
		publisherResolver: nil,
		close:             nil,
	}
}

// NewQueueFailedJobSrvFromConfig opens the DB from config and builds the service.
// The connection is owned by the service and closed via Close.
func NewQueueFailedJobSrvFromConfig(cfg Config, lgr Logger) (*QueueFailedJobSrv, error) {
	repo, err := NewQueueFailedJobRepo(cfg)
	if err != nil {
		return nil, err
	}

	srv := NewQueueFailedJobSrv(repo, lgr)
	srv.close = repo.Close

	return srv, nil
}

// SetConnection sets the connection field when recording a failed job
// and the expected connection for Retry without a PublisherResolver.
// An empty string is ignored.
func (s *QueueFailedJobSrv) SetConnection(name string) *QueueFailedJobSrv {
	if name != "" {
		s.connection = name
	}

	return s
}

// SetMaxRetryDuration sets MaxRetryDuration on republish.
// A non-positive value is ignored.
func (s *QueueFailedJobSrv) SetMaxRetryDuration(d time.Duration) *QueueFailedJobSrv {
	if d > 0 {
		s.maxRetryDuration = d
	}

	return s
}

// SetMaxRetryIDs sets the unique ID limit for a single Retry call.
// A non-positive value is ignored.
func (s *QueueFailedJobSrv) SetMaxRetryIDs(n int) *QueueFailedJobSrv {
	if n > 0 {
		s.maxRetryIDs = n
	}

	return s
}

// SetMaxRetryTimeout sets the overall timeout for one Retry call.
// A non-positive value is ignored.
func (s *QueueFailedJobSrv) SetMaxRetryTimeout(d time.Duration) *QueueFailedJobSrv {
	if d > 0 {
		s.maxRetryTimeout = d
	}

	return s
}

// SetHandlerContext sets the parent context for GetFailedJobHandler
// (cancellation on shutdown). Nil is ignored.
func (s *QueueFailedJobSrv) SetHandlerContext(ctx context.Context) *QueueFailedJobSrv {
	if ctx != nil {
		s.handlerStop = ctx.Done()
	}

	return s
}

// SetPublisherResolver sets broker selection by QueueFailedJob.Connection.
// Nil clears the resolver.
func (s *QueueFailedJobSrv) SetPublisherResolver(r PublisherResolver) *QueueFailedJobSrv {
	s.publisherResolver = r

	return s
}

// Close closes the DB connection if the service was created via NewQueueFailedJobSrvFromConfig.
func (s *QueueFailedJobSrv) Close() error {
	if s.close == nil {
		return nil
	}

	err := s.close()
	if err != nil {
		return fmt.Errorf("closing failed jobs service: %w", err)
	}

	s.close = nil

	return nil
}

// GetFailedJobHandler returns a handler that synchronously writes a failed job to the DB.
// Write errors are returned to the caller and also logged.
// A timeout context is derived from SetHandlerContext (default Background):
// the mq fail-handler contract does not pass a context.
func (s *QueueFailedJobSrv) GetFailedJobHandler() func(qName string, msg string, errMsg string) error {
	return func(qName string, msg string, errMsg string) error {
		parent, stopParent := contextFromDone(s.handlerStop)
		defer stopParent()

		ctx, cancel := context.WithTimeout(parent, s.dbWriteTimeout)
		defer cancel()

		err := s.repo.Add(ctx, QueueFailedJob{
			ID:                 0,
			Connection:         s.connection,
			Queue:              qName,
			Payload:            msg,
			Exception:          errMsg,
			FailedAt:           time.Now().Format(failedAtLayout),
			MaintenanceComment: "",
		})
		if err != nil {
			s.lgr.ErrorContext(ctx, "failed to add QueueFailedJob to database", "error", err)

			return fmt.Errorf("adding QueueFailedJob: %w", err)
		}

		return nil
	}
}

// GetByIDs returns failed jobs by identifiers.
func (s *QueueFailedJobSrv) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	queueFailedJobs, err := s.repo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("getting queue_failed_jobs: %w", err)
	}

	return queueFailedJobs, nil
}

// Add stores a failed job.
func (s *QueueFailedJobSrv) Add(ctx context.Context, queueFailedJob QueueFailedJob) error {
	err := s.repo.Add(ctx, queueFailedJob)
	if err != nil {
		return fmt.Errorf("adding queue_failed_jobs: %w", err)
	}

	return nil
}

// Delete removes a failed job.
func (s *QueueFailedJobSrv) Delete(ctx context.Context, queueFailedJob QueueFailedJob) error {
	err := s.repo.Delete(ctx, queueFailedJob)
	if err != nil {
		return fmt.Errorf("deleting queue_failed_jobs: %w", err)
	}

	return nil
}

// Retry republishes failed jobs and removes them from storage.
//
// Per record:
//  1. short existence check for all IDs;
//  2. Publish outside a transaction (correlation id gofailedjobs:failed-job:<id>);
//  3. SELECT FOR UPDATE + DELETE in a short transaction.
//
// An error on the k-th record does not roll back successfully processed 1..k-1.
// A crash after publish and before delete may duplicate a message — preferred over losing a job.
// Row locks are not held across network I/O.
func (s *QueueFailedJobSrv) Retry(ctx context.Context, ids []int, queue Publisher) error {
	if queue == nil {
		return ErrNilQueue
	}

	ids, err := normalizeIDs(ids, s.maxRetryIDs)
	if err != nil {
		return err
	}

	ctx, cancel := withOptionalTimeout(ctx, s.maxRetryTimeout)
	defer cancel()

	jobs, err := s.repo.GetByIDs(ctx, ids)
	if err != nil {
		return fmt.Errorf("retrying failed jobs: %w", err)
	}

	if missing := missingIDs(ids, jobs); len(missing) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingFailedJobs, missing)
	}

	for i := range jobs {
		if strings.TrimSpace(jobs[i].Queue) == "" {
			return fmt.Errorf("%w: id=%d", ErrEmptyQueueName, jobs[i].ID)
		}
	}

	for i := range jobs {
		err = s.retryOne(ctx, jobs[i], queue)
		if err != nil {
			return fmt.Errorf("retrying failed jobs (processed %d of %d): %w", i, len(jobs), err)
		}
	}

	return nil
}

func (s *QueueFailedJobSrv) retryOne(ctx context.Context, job QueueFailedJob, defaultQueue Publisher) error {
	pub, err := s.resolvePublisher(job, defaultQueue)
	if err != nil {
		return err
	}

	dur := s.maxRetryDuration
	pubCtx := SetCorrelationID(ctx, failedJobCorrelationID(job.ID))

	err = pub.Publish(pubCtx, mq.QueueName(job.Queue), mq.PublishMessage{
		Body:             []byte(job.Payload),
		ContentType:      mq.DefaultContentType,
		MaxRetryDuration: &dur,
		Priority:         nil,
	})
	if err != nil {
		return fmt.Errorf("publishing queueFailedJob id=%d: %w", job.ID, err)
	}

	err = s.repo.InTx(ctx, func(ctx context.Context, repo QueueFailedJobRepoInterface) error {
		locked, err := repo.GetByIDs(ctx, []int{job.ID})
		if err != nil {
			return fmt.Errorf("getting queue_failed_jobs id=%d: %w", job.ID, err)
		}

		if len(locked) == 0 {
			// Already deleted by a concurrent Retry — message may have been published twice (at-least-once).
			return nil
		}

		err = repo.Delete(ctx, locked[0])
		if err != nil {
			if errors.Is(err, ErrJobNotDeleted) {
				return nil
			}

			return fmt.Errorf("deleting queueFailedJob id=%d after publish: %w", job.ID, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("deleting published failed job id=%d: %w", job.ID, err)
	}

	return nil
}

//nolint:ireturn // broker is selected by connection; callers only need Publisher
func (s *QueueFailedJobSrv) resolvePublisher(job QueueFailedJob, defaultQueue Publisher) (Publisher, error) {
	conn := strings.TrimSpace(job.Connection)
	if conn == "" || conn == s.connection {
		return defaultQueue, nil
	}

	if s.publisherResolver != nil {
		pub, err := s.publisherResolver(conn)
		if err != nil {
			return nil, fmt.Errorf("publisher resolver for connection %q (id=%d): %w", conn, job.ID, err)
		}

		if pub == nil {
			return nil, fmt.Errorf("%w: connection=%q id=%d", ErrNilQueue, conn, job.ID)
		}

		return pub, nil
	}

	return nil, fmt.Errorf("%w: id=%d connection=%q (expected %q)", ErrConnectionMismatch, job.ID, conn, s.connection)
}

func failedJobCorrelationID(id int) string {
	return fmt.Sprintf("%s%d", failedJobCorrIDPrefix, id)
}

func withOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, timeout)
}

func contextFromDone(stop <-chan struct{}) (context.Context, context.CancelFunc) {
	if stop == nil {
		return context.Background(), func() {}
	}

	ctx, cancel := context.WithCancel(context.Background())

	select {
	case <-stop:
		cancel()

		return ctx, cancel
	default:
	}

	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

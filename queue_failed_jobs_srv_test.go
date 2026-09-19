package gofailedjobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	m.Called(ctx, msg, args)
}

type MockQueueFailedJobRepo struct {
	mock.Mock
}

func (m *MockQueueFailedJobRepo) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	args := m.Called(ctx, ids)

	return args.Get(0).([]QueueFailedJob), args.Error(1)
}

func (m *MockQueueFailedJobRepo) Add(ctx context.Context, queueFailedJob QueueFailedJob) error {
	args := m.Called(ctx, queueFailedJob)

	return args.Error(0)
}

func (m *MockQueueFailedJobRepo) Delete(ctx context.Context, queueFailedJob QueueFailedJob) error {
	args := m.Called(ctx, queueFailedJob)

	return args.Error(0)
}

func (m *MockQueueFailedJobRepo) InTx(
	ctx context.Context,
	fn func(context.Context, QueueFailedJobRepoInterface) error,
) error {
	return fn(ctx, m)
}

type fakeRepo struct {
	mu      sync.Mutex
	jobs    map[int]QueueFailedJob
	getErr  error
	addErr  error
	delErr  error
	deleted []int
}

func newFakeRepo(jobs ...QueueFailedJob) *fakeRepo {
	m := make(map[int]QueueFailedJob, len(jobs))
	for _, job := range jobs {
		m[job.ID] = job
	}

	return &fakeRepo{jobs: m}
}

func (f *fakeRepo) InTx(ctx context.Context, fn func(context.Context, QueueFailedJobRepoInterface) error) error {
	return fn(ctx, f)
}

func (f *fakeRepo) GetByIDs(_ context.Context, ids []int) ([]QueueFailedJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.getErr != nil {
		return nil, f.getErr
	}

	out := make([]QueueFailedJob, 0, len(ids))
	for _, id := range ids {
		if job, ok := f.jobs[id]; ok {
			out = append(out, job)
		}
	}

	return out, nil
}

func (f *fakeRepo) Add(_ context.Context, job QueueFailedJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.addErr != nil {
		return f.addErr
	}

	f.jobs[job.ID] = job

	return nil
}

func (f *fakeRepo) Delete(_ context.Context, job QueueFailedJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delErr != nil {
		return f.delErr
	}

	if _, ok := f.jobs[job.ID]; !ok {
		return fmt.Errorf("%w: id=%d", ErrJobNotDeleted, job.ID)
	}

	delete(f.jobs, job.ID)
	f.deleted = append(f.deleted, job.ID)

	return nil
}

func TestQueueFailedJobSrv_GetFailedJobHandler(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	mockLogger := new(MockLogger)
	srv := NewQueueFailedJobSrv(mockRepo, mockLogger).SetConnection("amqp")

	handler := srv.GetFailedJobHandler()
	require.NotNil(t, handler)

	qName := "test_queue"
	msg := "test_message"
	errMsg := "test_error"

	mockRepo.On("Add", mock.MatchedBy(func(ctx context.Context) bool {
		_, ok := ctx.Deadline()

		return ok
	}), mock.MatchedBy(func(job QueueFailedJob) bool {
		return job.Connection == "amqp" &&
			job.Queue == qName &&
			job.Payload == msg &&
			job.Exception == errMsg
	})).Return(nil).Once()

	err := handler(qName, msg, errMsg)
	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestQueueFailedJobSrv_GetFailedJobHandler_addError(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	mockLogger := new(MockLogger)
	srv := NewQueueFailedJobSrv(mockRepo, mockLogger)

	addErr := errors.New("db down")
	mockRepo.On("Add", mock.Anything, mock.AnythingOfType("QueueFailedJob")).Return(addErr).Once()
	mockLogger.On("ErrorContext", mock.Anything, "failed to add QueueFailedJob to database", mock.Anything).Once()

	err := srv.GetFailedJobHandler()("q", "m", "e")
	require.Error(t, err)
	assert.ErrorIs(t, err, addErr)
	mockRepo.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestQueueFailedJobSrv_GetByIDs(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	mockLogger := new(MockLogger)
	srv := NewQueueFailedJobSrv(mockRepo, mockLogger)

	tests := []struct {
		name    string
		ids     []int
		mock    func()
		want    int
		wantErr bool
	}{
		{
			name: "valid ids",
			ids:  []int{1, 2, 3},
			mock: func() {
				mockRepo.On("GetByIDs", mock.Anything, []int{1, 2, 3}).Return([]QueueFailedJob{
					{ID: 1}, {ID: 2}, {ID: 3},
				}, nil)
			},
			want:    3,
			wantErr: false,
		},
		{
			name: "empty ids",
			ids:  []int{},
			mock: func() {
				mockRepo.On("GetByIDs", mock.Anything, []int{}).Return([]QueueFailedJob{}, nil)
			},
			want:    0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mock()
			got, err := srv.GetByIDs(context.Background(), tt.ids)
			if tt.wantErr {
				assert.Error(t, err)

				return
			}
			assert.NoError(t, err)
			assert.Len(t, got, tt.want)
		})
	}
}

func TestQueueFailedJobSrv_Delete(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	mockLogger := new(MockLogger)
	srv := NewQueueFailedJobSrv(mockRepo, mockLogger)

	job := QueueFailedJob{
		ID:         1,
		Connection: "test",
		Queue:      "test_queue",
	}

	mockRepo.On("Delete", mock.Anything, job).Return(nil)

	err := srv.Delete(context.Background(), job)
	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestQueueFailedJobSrv_Add(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	srv := NewQueueFailedJobSrv(mockRepo, new(MockLogger))

	job := QueueFailedJob{ID: 1, Queue: "q"}
	mockRepo.On("Add", mock.Anything, job).Return(nil)

	require.NoError(t, srv.Add(context.Background(), job))
	mockRepo.AssertExpectations(t)
}

func TestQueueFailedJobSrv_Close_noop(t *testing.T) {
	srv := NewQueueFailedJobSrv(new(MockQueueFailedJobRepo), new(MockLogger))
	assert.NoError(t, srv.Close())
}

func TestNewQueueFailedJobSrvFromConfig_unsupportedDriver(t *testing.T) {
	srv, err := NewQueueFailedJobSrvFromConfig(Config{Driver: "unknown", Dialect: DialectMySQL}, new(MockLogger))
	require.ErrorIs(t, err, ErrUnsupportedDriver)
	assert.Nil(t, srv)
}

func TestNewQueueFailedJobSrvFromConfig_unsupportedDialect(t *testing.T) {
	srv, err := NewQueueFailedJobSrvFromConfig(Config{Driver: DriverGorm, Dialect: "oracle"}, new(MockLogger))
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	assert.Nil(t, srv)
}

func TestQueueFailedJobSrv_Retry_success(t *testing.T) {
	job := QueueFailedJob{ID: 10, Queue: "tasks.email", Payload: `{"k":1}`}
	repo := newFakeRepo(job)
	srv := NewQueueFailedJobSrv(repo, new(MockLogger)).SetMaxRetryDuration(time.Minute)
	q := &mockQueue{}

	err := srv.Retry(context.Background(), []int{10, 10}, q)
	require.NoError(t, err)
	assert.Empty(t, repo.jobs)
	require.Len(t, q.published, 1)
	assert.Equal(t, mq.QueueName(job.Queue), q.published[0].queue)
	assert.Equal(t, job.Payload, string(q.published[0].msg.Body))
	require.NotNil(t, q.published[0].msg.MaxRetryDuration)
	assert.Equal(t, time.Minute, *q.published[0].msg.MaxRetryDuration)
}

func TestQueueFailedJobSrv_Retry_publishErrorKeepsJob(t *testing.T) {
	job := QueueFailedJob{ID: 1, Queue: "q", Payload: "p"}
	repo := newFakeRepo(job)
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	pubErr := errors.New("broker unavailable")
	q := &mockQueue{publishErr: pubErr}

	err := srv.Retry(context.Background(), []int{1}, q)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	_, ok := repo.jobs[1]
	assert.True(t, ok, "row should remain in DB")
	assert.Empty(t, repo.deleted)
}

func TestQueueFailedJobSrv_Retry_missingJobs(t *testing.T) {
	repo := newFakeRepo(QueueFailedJob{ID: 1, Queue: "q", Payload: "p"})
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	q := &mockQueue{}

	err := srv.Retry(context.Background(), []int{1, 2}, q)
	require.ErrorIs(t, err, ErrMissingFailedJobs)
	assert.Empty(t, q.published)
	assert.Contains(t, repo.jobs, 1)
}

func TestQueueFailedJobSrv_Retry_emptyQueueName(t *testing.T) {
	repo := newFakeRepo(QueueFailedJob{ID: 1, Queue: "  ", Payload: "p"})
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	q := &mockQueue{}

	err := srv.Retry(context.Background(), []int{1}, q)
	require.ErrorIs(t, err, ErrEmptyQueueName)
	assert.Empty(t, q.published)
	assert.Contains(t, repo.jobs, 1)
}

func TestQueueFailedJobSrv_Retry_deleteErrorAfterPublish(t *testing.T) {
	job := QueueFailedJob{ID: 1, Queue: "q", Payload: "p"}
	repo := newFakeRepo(job)
	repo.delErr = errors.New("delete failed")
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	q := &mockQueue{}

	err := srv.Retry(context.Background(), []int{1}, q)
	require.Error(t, err)
	require.Len(t, q.published, 1)
	assert.Contains(t, repo.jobs, 1, "row should remain after delete error")
}

func TestQueueFailedJobSrv_Retry_batchPublishErrorKeepsRest(t *testing.T) {
	repo := newFakeRepo(
		QueueFailedJob{ID: 1, Queue: "q", Payload: "a"},
		QueueFailedJob{ID: 2, Queue: "q", Payload: "b"},
		QueueFailedJob{ID: 3, Queue: "q", Payload: "c"},
	)
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	pubErr := errors.New("broker unavailable")
	q := &mockQueue{publishErrAfter: 1, publishErr: pubErr}

	err := srv.Retry(context.Background(), []int{1, 2, 3}, q)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	require.Len(t, q.published, 1)
	assert.NotContains(t, repo.jobs, 1, "successful row is deleted and not rolled back")
	assert.Contains(t, repo.jobs, 2)
	assert.Contains(t, repo.jobs, 3)
}

func TestQueueFailedJobSrv_Retry_connectionMismatch(t *testing.T) {
	repo := newFakeRepo(QueueFailedJob{ID: 1, Connection: "other", Queue: "q", Payload: "p"})
	srv := NewQueueFailedJobSrv(repo, new(MockLogger)).SetConnection("rabbitmq")
	err := srv.Retry(context.Background(), []int{1}, &mockQueue{})
	require.ErrorIs(t, err, ErrConnectionMismatch)
}

func TestQueueFailedJobSrv_Retry_publisherResolver(t *testing.T) {
	repo := newFakeRepo(QueueFailedJob{ID: 1, Connection: "amqp2", Queue: "q", Payload: "p"})
	alt := &mockQueue{}
	srv := NewQueueFailedJobSrv(repo, new(MockLogger)).
		SetConnection("rabbitmq").
		SetPublisherResolver(func(connection string) (Publisher, error) {
			assert.Equal(t, "amqp2", connection)
			return alt, nil
		})

	require.NoError(t, srv.Retry(context.Background(), []int{1}, &mockQueue{}))
	require.Len(t, alt.published, 1)
	assert.Empty(t, repo.jobs)
}

func TestQueueFailedJobSrv_Retry_setsCorrelationID(t *testing.T) {
	repo := newFakeRepo(QueueFailedJob{ID: 42, Queue: "q", Payload: "p"})
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))
	q := &mockQueue{}

	require.NoError(t, srv.Retry(context.Background(), []int{42}, q))
	require.Len(t, q.published, 1)
	assert.Equal(t, failedJobCorrelationID(42), q.published[0].corrID)
}

func TestQueueFailedJobSrv_Retry_nilQueue(t *testing.T) {
	srv := NewQueueFailedJobSrv(newFakeRepo(), new(MockLogger))
	err := srv.Retry(context.Background(), []int{1}, nil)
	require.ErrorIs(t, err, ErrNilQueue)
}

func TestNewQueueFailedJobSrv_nilLogger(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	srv := NewQueueFailedJobSrv(mockRepo, nil)
	require.NotNil(t, srv)

	addErr := errors.New("db down")
	mockRepo.On("Add", mock.Anything, mock.AnythingOfType("QueueFailedJob")).Return(addErr).Once()

	err := srv.GetFailedJobHandler()("q", "m", "e")
	require.Error(t, err)
	assert.ErrorIs(t, err, addErr)
	mockRepo.AssertExpectations(t)
}

func TestQueueFailedJobSrv_GetFailedJobHandler_respectsHandlerContext(t *testing.T) {
	mockRepo := new(MockQueueFailedJobRepo)
	mockLogger := new(MockLogger)
	srv := NewQueueFailedJobSrv(mockRepo, mockLogger)

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	srv.SetHandlerContext(parent)

	mockRepo.On("Add", mock.MatchedBy(func(ctx context.Context) bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}), mock.AnythingOfType("QueueFailedJob")).Return(context.Canceled).Once()
	mockLogger.On("ErrorContext", mock.Anything, "failed to add QueueFailedJob to database", mock.Anything).Once()

	err := srv.GetFailedJobHandler()("q", "m", "e")
	require.Error(t, err)
	mockRepo.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestQueueFailedJobSrv_Retry_invalidIDs(t *testing.T) {
	srv := NewQueueFailedJobSrv(newFakeRepo(), new(MockLogger))
	q := &mockQueue{}

	err := srv.Retry(context.Background(), []int{0, -1}, q)
	require.ErrorIs(t, err, ErrInvalidJobID)
	assert.Empty(t, q.published)
}

func TestQueueFailedJobSrv_Retry_tooManyIDs(t *testing.T) {
	srv := NewQueueFailedJobSrv(newFakeRepo(), new(MockLogger)).SetMaxRetryIDs(2)
	ids := []int{1, 2, 3}
	err := srv.Retry(context.Background(), ids, &mockQueue{})
	require.ErrorIs(t, err, ErrTooManyIDs)
}

func TestQueueFailedJobSrv_Retry_emptyList(t *testing.T) {
	srv := NewQueueFailedJobSrv(newFakeRepo(), new(MockLogger))
	err := srv.Retry(context.Background(), nil, &mockQueue{})
	require.ErrorIs(t, err, ErrEmptyIDList)
}

func TestQueueFailedJobSrv_Retry_concurrentSameID(t *testing.T) {
	job := QueueFailedJob{ID: 7, Queue: "q", Payload: "p"}
	repo := newFakeRepo(job)
	srv := NewQueueFailedJobSrv(repo, new(MockLogger))

	started := make(chan struct{})
	release := make(chan struct{})
	q1 := &mockQueue{beforePublish: func() {
		close(started)
		<-release
	}}
	q2 := &mockQueue{}

	errCh := make(chan error, 2)
	go func() {
		errCh <- srv.Retry(context.Background(), []int{7}, q1)
	}()
	<-started
	go func() {
		errCh <- srv.Retry(context.Background(), []int{7}, q2)
	}()
	close(release)

	err1 := <-errCh
	err2 := <-errCh

	var sawOK, sawMissing bool
	for _, err := range []error{err1, err2} {
		switch {
		case err == nil:
			sawOK = true
		case errors.Is(err, ErrMissingFailedJobs):
			sawMissing = true
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	assert.True(t, sawOK, "at least one Retry should succeed")
	assert.True(t, sawOK && (sawMissing || len(q1.published)+len(q2.published) >= 1))
	assert.Empty(t, repo.jobs)
	assert.GreaterOrEqual(t, len(q1.published)+len(q2.published), 1)
}

func TestNormalizeIDs(t *testing.T) {
	t.Parallel()

	got, err := normalizeIDs([]int{1, 1, 2}, 10)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, got)

	_, err = normalizeIDs([]int{1, 0}, 10)
	require.ErrorIs(t, err, ErrInvalidJobID)

	_, err = normalizeIDs(nil, 10)
	require.ErrorIs(t, err, ErrEmptyIDList)
}

var (
	_ QueueFailedJobRepoInterface = (*fakeRepo)(nil)
	_ Publisher                   = (*mockQueue)(nil)
	_ mq.Queue                    = (*mockQueue)(nil)
)

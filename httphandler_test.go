package gofailedjobs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	mq "github.com/ivan-makarenkov/go-amqp-adapter"
	"github.com/labstack/echo/v4"
)

func TestSplitIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    []int
		wantErr error
	}{
		{name: "single id", input: "42", want: []int{42}},
		{name: "comma-separated", input: "1,2,3", want: []int{1, 2, 3}},
		{name: "spaces around and between", input: "  7, 8  ", want: []int{7, 8}},
		{name: "empty tokens", input: "1,,2", want: []int{1, 2}},
		{name: "duplicates", input: "1,1,2", want: []int{1, 2}},
		{name: "empty after trim", input: "   ", wantErr: ErrEmptyIDList},
		{name: "commas only", input: ",,", wantErr: ErrEmptyIDList},
		{name: "not a number", input: "x", wantErr: ErrInvalidJobID},
		{name: "mixed garbage", input: "1,foo", wantErr: ErrInvalidJobID},
		{name: "zero", input: "0", wantErr: ErrInvalidJobID},
		{name: "negative", input: "-3", wantErr: ErrInvalidJobID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := splitIDs(tt.input)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitIDs: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitIDs_tooMany(t *testing.T) {
	t.Parallel()

	parts := make([]string, maxRetryIDs+1)
	for i := range parts {
		parts[i] = strconv.Itoa(i + 1)
	}
	_, err := splitIDs(strings.Join(parts, ","))
	if !errors.Is(err, ErrTooManyIDs) {
		t.Fatalf("expected ErrTooManyIDs, got %v", err)
	}
}

func TestSplitIDs_duplicatesUnderLimit(t *testing.T) {
	t.Parallel()

	parts := make([]string, maxRetryIDs+1)
	for i := range parts {
		parts[i] = "1"
	}
	got, err := splitIDs(strings.Join(parts, ","))
	if err != nil {
		t.Fatalf("duplicates must not exceed limit: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("got %v", got)
	}
}

// mockFailedJobSrv stubs QueueFailedJobSrvInterface for Handler.
type mockFailedJobSrv struct {
	retryErr   error
	retriedIDs []int
	gotQueue   Publisher
}

func (m *mockFailedJobSrv) GetFailedJobHandler() func(qName string, msg string, errMsg string) error {
	return func(qName string, msg string, errMsg string) error { return nil }
}

func (m *mockFailedJobSrv) GetByIDs(ctx context.Context, ids []int) ([]QueueFailedJob, error) {
	return nil, nil
}

func (m *mockFailedJobSrv) Add(ctx context.Context, job QueueFailedJob) error {
	return nil
}

func (m *mockFailedJobSrv) Delete(ctx context.Context, job QueueFailedJob) error {
	return nil
}

func (m *mockFailedJobSrv) Retry(ctx context.Context, ids []int, queue Publisher) error {
	m.retriedIDs = append([]int(nil), ids...)
	m.gotQueue = queue
	return m.retryErr
}

// mockQueue stubs mq.Queue for Handler and Retry.
type mockQueue struct {
	publishErr      error
	publishErrAfter int // return publishErr after this many successful Publishes (0 = immediately)
	publishCount    int
	beforePublish   func()
	published       []struct {
		queue  mq.QueueName
		msg    mq.PublishMessage
		corrID string
	}
}

func (m *mockQueue) Publish(ctx context.Context, queueName mq.QueueName, msg mq.PublishMessage) error {
	if m.beforePublish != nil {
		m.beforePublish()
	}
	if m.publishErr != nil && m.publishCount >= m.publishErrAfter {
		return m.publishErr
	}
	m.publishCount++
	m.published = append(m.published, struct {
		queue  mq.QueueName
		msg    mq.PublishMessage
		corrID string
	}{queueName, msg, GetCorrelationID(ctx)})
	return nil
}

func (m *mockQueue) AddConsumer(ctx context.Context, queueName mq.QueueName, handler mq.ConsumerHandler) error {
	return nil
}

func (m *mockQueue) AddConsumerN(ctx context.Context, queueName mq.QueueName, parallelism int, handler mq.ConsumerHandler) error {
	return nil
}

func (m *mockQueue) InitConsumer(ctx context.Context) error { return nil }

func (m *mockQueue) Shutdown(ctx context.Context) error { return nil }

func newHandlerContext(path string) (echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	return e.NewContext(req, rec), rec
}

func TestHandler_retryError(t *testing.T) {
	t.Parallel()
	srv := &mockFailedJobSrv{retryErr: errors.New("db unavailable")}
	c, rec := newHandlerContext("/retry-task?task=1")
	err := NewHandler(srv, &mockQueue{})(c)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestHandler_clientErrorStatus(t *testing.T) {
	t.Parallel()
	srv := &mockFailedJobSrv{retryErr: ErrMissingFailedJobs}
	c, rec := newHandlerContext("/retry-task?task=1")
	err := NewHandler(srv, &mockQueue{})(c)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestHandler_success(t *testing.T) {
	t.Parallel()
	srv := &mockFailedJobSrv{}
	mqMock := &mockQueue{}
	c, rec := newHandlerContext("/retry-task?task=10,10")
	err := NewHandler(srv, mqMock)(c)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Fatalf("response %d %q", rec.Code, rec.Body.String())
	}
	if !reflect.DeepEqual(srv.retriedIDs, []int{10}) {
		t.Fatalf("Retry ids: %v", srv.retriedIDs)
	}
	if srv.gotQueue != mqMock {
		t.Fatal("Retry must receive the queue from NewHandler")
	}
}

func TestHandler_invalidIDs(t *testing.T) {
	t.Parallel()
	c, rec := newHandlerContext("/retry-task?task=not-int")
	err := NewHandler(&mockFailedJobSrv{}, &mockQueue{})(c)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), ErrInvalidJobID.Error()) {
		t.Fatalf("body must contain ErrInvalidJobID, got %q", rec.Body.String())
	}
}

func TestHandler_emptyTask(t *testing.T) {
	t.Parallel()
	c, rec := newHandlerContext("/retry-task")
	err := NewHandler(&mockFailedJobSrv{}, &mockQueue{})(c)
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()
	e := echo.New()
	srv := &mockFailedJobSrv{}
	q := &mockQueue{}
	Register(e, PassThroughMiddleware, srv, q)

	req := httptest.NewRequest(http.MethodPost, "/retry-task?task=1", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
		t.Fatalf("response %d %q", rec.Code, rec.Body.String())
	}
}

func TestRegister_nilMiddlewarePanics(t *testing.T) {
	t.Parallel()
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("expected panic")
		}
		if !errors.Is(rec.(error), ErrNilAuthMiddleware) {
			t.Fatalf("expected ErrNilAuthMiddleware, got %v", rec)
		}
	}()
	Register(echo.New(), nil, &mockFailedJobSrv{}, &mockQueue{})
}

func TestNewHandler_nilServicePanics(t *testing.T) {
	t.Parallel()
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("expected panic")
		}
		if !errors.Is(rec.(error), ErrNilService) {
			t.Fatalf("expected ErrNilService, got %v", rec)
		}
	}()
	_ = NewHandler(nil, &mockQueue{})
}

func TestNewHandler_nilQueuePanics(t *testing.T) {
	t.Parallel()
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatal("expected panic")
		}
		if !errors.Is(rec.(error), ErrNilQueue) {
			t.Fatalf("expected ErrNilQueue, got %v", rec)
		}
	}()
	_ = NewHandler(&mockFailedJobSrv{}, nil)
}

func TestDefaultMaxRetryDuration_positive(t *testing.T) {
	t.Parallel()
	if defaultMaxRetryDuration <= 0 {
		t.Fatal("duration must be positive")
	}
}

func TestUniqueAndMissingIDs(t *testing.T) {
	t.Parallel()

	if got := uniqueIDs([]int{1, 1, 2, 2, 3}); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("uniqueIDs: %v", got)
	}

	missing := missingIDs([]int{1, 2, 3}, []QueueFailedJob{{ID: 1}, {ID: 3}})
	if !reflect.DeepEqual(missing, []int{2}) {
		t.Fatalf("missingIDs: %v", missing)
	}
}

var _ QueueFailedJobSrvInterface = (*mockFailedJobSrv)(nil)

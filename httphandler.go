package gofailedjobs

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
)

// RQ holds query parameters for POST /retry-task.
type RQ struct {
	IDs string `query:"task"`
}

// RouteRegistrar is Echo or a Group that can register routes.
type RouteRegistrar interface {
	POST(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
}

// Register registers POST /retry-task.
// authMiddleware is required (auth/ACL); nil panics with ErrNilAuthMiddleware.
// If auth is already applied globally on Echo, pass PassThroughMiddleware.
func Register(
	e RouteRegistrar,
	authMiddleware echo.MiddlewareFunc,
	srv QueueFailedJobSrvInterface,
	queue Publisher,
) {
	if authMiddleware == nil {
		panic(ErrNilAuthMiddleware)
	}

	e.POST(RoutePath, NewHandler(srv, queue), authMiddleware)
}

// PassThroughMiddleware is a no-op middleware when auth is already applied at the app level.
func PassThroughMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return next
}

// NewHandler returns an Echo handler that retries failed jobs.
// srv and queue are required; nil panics.
func NewHandler(srv QueueFailedJobSrvInterface, queue Publisher) echo.HandlerFunc {
	if srv == nil {
		panic(ErrNilService)
	}
	if queue == nil {
		panic(ErrNilQueue)
	}

	return func(c echo.Context) error {
		ids, err := splitIDs(c.QueryParam("task"))
		if err != nil {
			return writeClientErr(c, err)
		}

		err = srv.Retry(c.Request().Context(), ids, queue)
		if err != nil {
			if code, ok := clientErrorStatus(err); ok {
				return c.String(code, err.Error())
			}

			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "OK")
	}
}

func clientErrorStatus(err error) (int, bool) {
	switch {
	case errors.Is(err, ErrMissingFailedJobs):
		return http.StatusNotFound, true
	case errors.Is(err, ErrEmptyIDList),
		errors.Is(err, ErrInvalidJobID),
		errors.Is(err, ErrTooManyIDs),
		errors.Is(err, ErrEmptyQueueName),
		errors.Is(err, ErrConnectionMismatch),
		errors.Is(err, ErrNilQueue):
		return http.StatusBadRequest, true
	default:
		return 0, false
	}
}

func writeClientErr(c echo.Context, err error) error {
	code, ok := clientErrorStatus(err)
	if !ok {
		code = http.StatusBadRequest
	}

	return c.String(code, err.Error())
}

func splitIDs(ids string) ([]int, error) {
	ids = strings.TrimSpace(ids)
	if ids == "" {
		return nil, ErrEmptyIDList
	}

	tmp := strings.Split(ids, ",")
	result := make([]int, 0, len(tmp))

	for _, raw := range tmp {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		value, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", ErrInvalidJobID, raw, err)
		}

		result = append(result, value)
	}

	return normalizeIDs(result, maxRetryIDs)
}

package gofailedjobs

import "errors"

// ErrMissingFailedJobs is returned when some requested failed job IDs were not found.
var ErrMissingFailedJobs = errors.New("not all failed jobs were found")

// ErrEmptyIDList is returned when the ID list is empty.
var ErrEmptyIDList = errors.New("empty id list")

// ErrInvalidJobID is returned when a failed job ID is non-positive or not a number.
var ErrInvalidJobID = errors.New("invalid failed job id")

// ErrTooManyIDs is returned when the number of unique IDs exceeds the limit.
var ErrTooManyIDs = errors.New("too many ids")

// ErrEmptyQueueName is returned when a failed job has an empty queue name.
var ErrEmptyQueueName = errors.New("empty queue name")

// ErrJobNotDeleted is returned when DELETE affected no rows.
var ErrJobNotDeleted = errors.New("failed job was not deleted")

// ErrNilQueue is returned when Retry or NewHandler is called without a queue.
var ErrNilQueue = errors.New("queue is not set")

// ErrNilService is returned when NewHandler or Register is called without a service.
var ErrNilService = errors.New("failed jobs service is not set")

// ErrConnectionMismatch is returned when a job connection does not match and no resolver is set.
var ErrConnectionMismatch = errors.New("failed job connection mismatch")

// ErrNilAuthMiddleware is returned when Register is called without auth middleware.
var ErrNilAuthMiddleware = errors.New("auth middleware is required for /retry-task")

// ErrUnsupportedDriver is returned when Config.Driver is unknown.
var ErrUnsupportedDriver = errors.New("unsupported database driver")

// ErrUnsupportedDialect is returned when Config.Dialect is unknown.
var ErrUnsupportedDialect = errors.New("unsupported database dialect")

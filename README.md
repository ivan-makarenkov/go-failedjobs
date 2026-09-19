# go-failedjobs

Retry failed queue jobs: load them from MySQL/Postgres and republish payloads to RabbitMQ via an HTTP endpoint.

```bash
go get github.com/ivan-makarenkov/go-failedjobs
```

Package: `gofailedjobs`.

## Together with amqp-adapter

This library and [amqp-adapter](https://github.com/ivan-makarenkov/amqp-adapter) were designed to be used together as a replacement for Laravel's retry-through-queue mechanism (`tries` / delayed retries → `failed_jobs` → `php artisan queue:retry`).

- **amqp-adapter** handles in-broker retries (delay queue / DLX) — the analogue of Laravel's automatic job retries.
- When retries are exhausted (or the handler returns a non-retryable error), `WithFailHandler` writes the payload here via `GetFailedJobHandler` — the analogue of Laravel's `failed_jobs` table.
- **go-failedjobs** later republishes selected rows to RabbitMQ (`POST /retry-task` / `QueueFailedJobSrv.Retry`) — the analogue of `php artisan queue:retry`. A row is deleted from storage only after a successful publish.

The package provides an HTTP route and `QueueFailedJobSrv.Retry` to select failed-job rows by ID and republish payloads to RabbitMQ with a configurable `MaxRetryDuration` (default 20 minutes).

Failed jobs can be stored with [GORM](https://github.com/go-gorm/gorm) or [sqlx](https://github.com/jmoiron/sqlx) — choose via `gofailedjobs.Config.Driver`. DBMS is MySQL or PostgreSQL via `gofailedjobs.Config.Dialect`.

## Requirements

- **Go** at least the version in `go.mod` (currently `1.25.0`).

## Direct dependencies

An application that imports `github.com/ivan-makarenkov/go-failedjobs` should pull the same contracts the package uses. Direct `require` entries:

| Module | Purpose |
|--------|---------|
| `github.com/ivan-makarenkov/amqp-adapter` | `Queue` / `Publisher` interface (broker publish) |
| `github.com/labstack/echo/v4` | HTTP handler `Register` / `NewHandler` |

Transitive dependencies (GORM, sqlx, AMQP, etc.) are pulled automatically — declare them in the consumer `go.mod` only if used directly.

Logger is the `gofailedjobs.Logger` interface (`ErrorContext` is enough); `*slog.Logger` works. `nil` is replaced with a no-op.

On `Retry`, the correlation id is written to context via `gofailedjobs.SetCorrelationID` (`gofailedjobs:failed-job:<id>`). To put it into AMQP headers, configure the queue, e.g. with `amqp-adapter/otel`:

```go
import (
    "log/slog"

    mq "github.com/ivan-makarenkov/amqp-adapter"
    "github.com/ivan-makarenkov/amqp-adapter/otel"
    "github.com/ivan-makarenkov/go-failedjobs"
)

cfg := otel.PropagationConfig{
    CorrelationIDKey: "x-correlation-id",
    GetCorrelationID: gofailedjobs.GetCorrelationID,
    SetCorrelationID: gofailedjobs.SetCorrelationID,
}
queue, err := mq.New(mqConf,
    mq.WithPublishHeadersBuilder(otel.NewPublishHeadersBuilder(cfg)),
    mq.WithLogger(slog.Default()),
)
```

## Table schema

DDL: [`schema.sql`](schema.sql). The package does not run migrations — create `queue_failed_jobs` before use.

## Choosing library and DBMS

`Driver` is GORM or sqlx (empty = gorm). `Dialect` is required: `mysql` or `postgres` (aliases `mariadb`, `postgresql`, `pg` are accepted).

```go
repo, err := gofailedjobs.NewQueueFailedJobRepo(gofailedjobs.Config{
    Driver:            gofailedjobs.DriverGorm, // or gofailedjobs.DriverSqlx; empty = gorm
    Dialect:           gofailedjobs.DialectPostgres, // or gofailedjobs.DialectMySQL
    FormatDSN:         "postgres://user:pass@127.0.0.1:5432/dbname?sslmode=disable",
    // MySQL: "user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true"
    DBMaxOpenConns:    10,
    DBMaxIdleConns:    5,
    DBConnMaxLifetime: time.Hour,
    DBPingTimeout:     5 * time.Second, // optional, default 5s
})
if err != nil {
    log.Fatal(err)
}
defer repo.Close()

srv := gofailedjobs.NewQueueFailedJobSrv(repo, slog.Default())
// optional:
// srv.SetConnection("rabbitmq")
// srv.SetMaxRetryDuration(10 * time.Minute)
// srv.SetMaxRetryTimeout(2 * time.Minute)
// srv.SetMaxRetryIDs(50)
// srv.SetHandlerContext(appCtx) // cancel failed-job writes on shutdown
// srv.SetPublisherResolver(func(conn string) (gofailedjobs.Publisher, error) { ... })
```

If the application already opened a connection:

```go
repo := gofailedjobs.NewGormQueueFailedJobRepo(gormDB)
// or
repo := gofailedjobs.NewSqlxQueueFailedJobRepo(sqlxDB)
```

From config (connection owned by the service — close via `Close`):

```go
srv, err := gofailedjobs.NewQueueFailedJobSrvFromConfig(cfg, slog.Default())
if err != nil {
    log.Fatal(err)
}
defer srv.Close()
```

If row `connection` differs from `SetConnection`, provide `SetPublisherResolver`, otherwise `Retry` returns `ErrConnectionMismatch`.

## Security

`Register(..., authMiddleware, ...)` **requires** auth/ACL middleware. `nil` panics (`ErrNilAuthMiddleware`). If authorization is already global on Echo, pass `gofailedjobs.PassThroughMiddleware`.

## Route

```go
e := echo.New()
gofailedjobs.Register(e, authMW, srv, queue)
// or manually:
// e.POST(gofailedjobs.RoutePath, gofailedjobs.NewHandler(srv, queue), authMW)
```

- **Method and path:** `POST /retry-task`
- **Query:** `task` — comma-separated integer IDs (e.g. `?task=1,2,3`), at most 100 unique, ID > 0
- **Behavior:** all IDs must exist (`ErrMissingFailedJobs` otherwise). Per row: publish outside a transaction (correlation id `gofailedjobs:failed-job:<id>`), then a short `SELECT FOR UPDATE` + `DELETE` transaction. An error on the k-th row does not roll back 1..k-1. A crash after publish and before delete may duplicate — preferred over losing a job. Client errors (`ErrInvalidJobID`, `ErrMissingFailedJobs`, …) map to HTTP 400/404; others to 500.
- **Timeout:** `Retry` defaults to 2 minutes (`SetMaxRetryTimeout`).

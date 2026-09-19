package gofailedjobs

import "context"

// nopLogger is a no-op Logger used when New* is called with nil.
type nopLogger struct{}

func (nopLogger) ErrorContext(context.Context, string, ...any) {}

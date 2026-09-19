package gofailedjobs

import "context"

// Logger is a minimal contract for failed-jobs service errors.
// *slog.Logger satisfies this interface.
type Logger interface {
	ErrorContext(ctx context.Context, msg string, args ...any)
}

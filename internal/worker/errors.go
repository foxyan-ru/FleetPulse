package worker

import "errors"

// ErrTransient is a sentinel that enrichment/processing errors should wrap
// (via fmt.Errorf("...: %w", ErrTransient)) when the failure is believed to
// be retryable -- a downstream timeout, a connection reset, a rate limit.
// Anything NOT wrapping ErrTransient is treated as a permanent failure and
// is not retried even if delivery attempts remain.
var ErrTransient = errors.New("transient processing failure")

// IsTransient reports whether err (or anything it wraps) is ErrTransient.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient)
}

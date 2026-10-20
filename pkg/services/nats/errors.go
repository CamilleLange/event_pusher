package nats

import "errors"

// ErrNilHandler is returned by Listen when no handler is given.
var ErrNilHandler = errors.New("nats: nil handler")

package redis

import "errors"

// ErrInvalidBatchSize is returned by New when Config.BatchSize is not positive.
var ErrInvalidBatchSize = errors.New("redis: batch size must be positive")

// ErrMissingPayload is returned by Empty when a stream entry does not carry the
// payload field written by Insert/InsertBatch (nothing is deleted).
var ErrMissingPayload = errors.New("redis: stream entry has no payload")

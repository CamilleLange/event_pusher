package s3

import "errors"

var (
	// ErrUnknownClientType is returned by New when Config.ClientType is not a known client type.
	ErrUnknownClientType = errors.New("s3: unknown client type")

	// ErrEmptyBucket is returned by New when Config.Bucket is empty.
	ErrEmptyBucket = errors.New("s3: empty bucket name")
)

package eventpusher

import "errors"

var (
	ErrStreamFull = errors.New("current stream is full")
)

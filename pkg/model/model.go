// Package model holds the event types shared by the producer and the service.
package model

type AvailableStreamListEvent []string

type StreamFullEvent struct {
	Name string `json:"stream_name"`
}

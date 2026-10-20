package nats

import (
	"fmt"

	n "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// New connects to NATS and builds the JetStream client on top of the connection.
func New(c *Config) (*Client, error) {
	conn, err := n.Connect(c.URL)
	if err != nil {
		return nil, fmt.Errorf("can't connect to Nats : %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("can't create Jetstream client from Nats connection : %w", err)
	}

	return &Client{
		conn: conn,
		js:   js,
	}, nil
}

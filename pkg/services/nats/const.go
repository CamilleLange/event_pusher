package nats

const (
	// Stream List

	// APPLICATION_STREAM is the JetStream reserved for Application messages.
	APPLICATION_STREAM = "ApplicationStream"
	// EVENT_PUSHER_STREAM is the JetStream reserved for internal messages.
	EVENT_PUSHER_STREAM = "EventPusherStream"

	// Subject list

	// APPLICATION_EVENT_SUBJECT is the subject of the APPLICATION_STREAM reserved to receive Application events.
	APPLICATION_EVENT_SUBJECT = "ApplicationEvent"
	// AVAILABLE_STREAM_LIST_SUBJECT is the subject of the EVENT_PUSHER_STREAM reserved to sync instances on wich redis stream is available to use.
	AVAILABLE_STREAM_LIST_SUBJECT = "AvailableStreamList"
	// STREAM_FULL_SUBJECT is the subject of the EVENT_PUSHER_STREAM reserved to indicate when a file need to be created.
	STREAM_FULL_SUBJECT = "StreamFull"
)

package jetstream

// Error is a sentinel a caller of this package can act on.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrNilJetStream    Error = "jetstream: nil jetstream"
	ErrNilCapture      Error = "jetstream: nil capture"
	ErrNilData         Error = "jetstream: nil data"
	ErrSubjectRequired Error = "jetstream: subject is required"
	ErrStreamRequired  Error = "jetstream: stream name and subject are required"
	ErrConsumeConfig   Error = "jetstream: stream, subject, durable, and skip origin are required"
)

package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/Deasel011/go-cdc"
	"github.com/Deasel011/go-cdc/data"
	"github.com/nats-io/nats.go/jetstream"
)

// EnsureStream creates or updates the CDC stream.
// The duplicate window is the value configured here (two minutes), not a
// server maximum. A republish after the window is a second message. Apply
// still ignores it when its sequence is older than the last applied change
// for that origin, table, and key.
func EnsureStream(ctx context.Context, js jetstream.JetStream, name, subject string) error {
	if js == nil {
		return ErrNilJetStream
	}
	if name == "" || subject == "" {
		return ErrStreamRequired
	}
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       name,
		Subjects:   []string{subject},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	return err
}

// Publish reads src in order and publishes each change. The JetStream message
// id is Change.ID. src deletes the outbox row only after Publish returns from
// handling that change, which is after the server accepts it.
func Publish(ctx context.Context, js jetstream.JetStream, subject string, src cdc.Capture) error {
	if js == nil {
		return ErrNilJetStream
	}
	if subject == "" {
		return ErrSubjectRequired
	}
	if src == nil {
		return ErrNilCapture
	}
	for change, err := range src.Changes(ctx) {
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		body, err := json.Marshal(change)
		if err != nil {
			return err
		}
		if _, err := js.Publish(ctx, subject, body, jetstream.WithMsgID(change.ID)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

// ConsumeConfig selects the stream and which origin to ignore.
type ConsumeConfig struct {
	Stream     string
	Subject    string
	Durable    string
	SkipOrigin string
}

// Consume applies messages to target until ctx is cancelled.
// A permanent error is terminated so it cannot block the consumer.
// A retryable error is negatively acknowledged with a delay.
// Messages from SkipOrigin are acknowledged and ignored, which stops an echo
// when both directions share a subject.
func Consume(ctx context.Context, js jetstream.JetStream, cfg ConsumeConfig, target cdc.Data) error {
	if js == nil {
		return ErrNilJetStream
	}
	if cfg.Stream == "" || cfg.Subject == "" || cfg.Durable == "" || cfg.SkipOrigin == "" {
		return ErrConsumeConfig
	}
	if target == nil {
		return ErrNilData
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxAckPending: 1,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		return err
	}
	iter, err := cons.Messages()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { iter.Stop() })
	defer func() {
		stop()
		iter.Stop()
	}()
	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := handle(ctx, msg, cfg.SkipOrigin, target); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func handle(ctx context.Context, msg jetstream.Msg, skip string, target cdc.Data) error {
	var ch cdc.Change
	if err := json.Unmarshal(msg.Data(), &ch); err != nil {
		slog.Error("cdc jetstream: term undecodable message", "err", err)
		return msg.Term()
	}
	if ch.Origin == skip {
		return msg.Ack()
	}
	if err := ch.Validate(); err != nil {
		slog.Error("cdc jetstream: term invalid change", "id", ch.ID, "err", err)
		return msg.Term()
	}
	err := target.Apply(ctx, ch)
	switch {
	case err == nil:
		return msg.Ack()
	case ctx.Err() != nil:
		return nil
	case data.IsRetryable(err):
		slog.Error("cdc jetstream: retry", "id", ch.ID, "err", err)
		return msg.NakWithDelay(200 * time.Millisecond)
	case errors.Is(err, data.ErrConflict), errors.Is(err, data.ErrIncompleteUpdate):
		slog.Error("cdc jetstream: term conflict", "id", ch.ID, "err", err)
		return msg.Term()
	default:
		slog.Error("cdc jetstream: term apply error", "id", ch.ID, "err", err)
		return msg.Term()
	}
}

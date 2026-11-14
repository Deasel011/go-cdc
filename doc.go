// Package cdc captures committed database/sql writes and replicates them
// through NATS JetStream.
//
// DB wraps a Postgres (pgx) or SQLite connection. A committed INSERT, UPDATE,
// or DELETE is stored in cdc_outbox in that same transaction, then published
// by Relay. Applier writes the event onto another database. Apply is
// idempotent, and an applier ignores its own origin, so the two databases can
// replicate to each other without echoing a change back and forth.
//
// Rows that already exist when DB is opened are not captured.
package cdc

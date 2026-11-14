# go-cdc

`cdc` is the vocabulary for replicating a database through a change stream. Postgres and SQLite both speak `database/sql`, and SQLite has no equivalent of Postgres logical replication, so changes are captured in the client, inside the transaction that made them. The same path runs in either direction.

## Packages

| Package | Role |
| --- | --- |
| `cdc` | `Change`, `Capture`, and `Data`. No drivers, no NATS. |
| `capture` | `database/sql` wrapper. Committed writes land in `cdc_outbox`. `Changes` reads that outbox in order. |
| `capture/internal/sqlparse` | The single-row INSERT/UPDATE/DELETE subset. |
| `data` | Applies a `Change` with a `Dialect`. |
| `data/postgres`, `data/sqlite` | Engine differences and the driver import. |
| `stream/jetstream` | Publishes a `Capture` to JetStream and consumes it into a `Data`. |

`modernc.org/sqlite` is the SQLite driver because it is pure Go, so `go test -race` does not need cgo.

## Wiring both directions

```go
pg, err := capture.Open(ctx, postgres.Dialect{}, capture.Config{
    DSN: dsn, Origin: "postgres",
    Tables: []capture.Table{{Name: "items", Key: []string{"id"}}},
})
lite, err := capture.Open(ctx, sqlite.Dialect{}, capture.Config{
    DSN: liteDSN, Origin: "sqlite",
    Tables: []capture.Table{{Name: "items", Key: []string{"id"}}},
})
pgStore, err := data.Open(ctx, pg.Std(), postgres.Dialect{})
liteStore, err := data.Open(ctx, lite.Std(), sqlite.Dialect{})

err = jetstream.EnsureStream(ctx, js, "CDC", "cdc.events")
go jetstream.Publish(ctx, js, "cdc.events", pg)
go jetstream.Publish(ctx, js, "cdc.events", lite)
go jetstream.Consume(ctx, js, jetstream.ConsumeConfig{
    Stream: "CDC", Subject: "cdc.events", Durable: "apply-on-sqlite", SkipOrigin: "sqlite",
}, liteStore)
go jetstream.Consume(ctx, js, jetstream.ConsumeConfig{
    Stream: "CDC", Subject: "cdc.events", Durable: "apply-on-postgres", SkipOrigin: "postgres",
}, pgStore)
```

Create the table on both sides yourself. Writes through `Std` are not captured; use that for DDL. Apply uses `Std` as well, so an applied row is not published again. Each consumer also ignores its own origin.

```go
tx, err := pg.BeginTx(ctx, nil)
_, err = tx.ExecContext(ctx, `INSERT INTO items (id, name) VALUES ($1, $2)`, 1, "a")
err = tx.Commit()
```

SQLite statements use `?`. The applier rewrites placeholders. An update or delete is captured only when it changes exactly one row. Zero rows publishes nothing. More than one row returns `capture.ErrMultipleRows`; the caller decides whether to roll back.

## Statements that are captured

Single-row only, values as placeholders (`$1` or `?`, not mixed, each argument once):

```sql
INSERT INTO items (id, name) VALUES ($1, $2)
UPDATE items SET name = $1 WHERE id = $2
DELETE FROM items WHERE id = $1
```

`WHERE` must be equality on the primary key, combined with `AND` when the key has more than one column. Identifiers are stored lowercase. A statement outside this subset fails the write.

## Adding an engine

Engine-specific behavior sits on `data.Dialect`: driver name, placeholders, identifier quoting, upsert SQL (Postgres and SQLite share `data.Upsert`), outbox DDL, applied-sequence DDL, DSN defaults, pool setup, retryable errors, and non-primary-key unique conflicts. Adding an engine is a new package next to `data/postgres` that implements `Dialect` and blank-imports its `database/sql` driver. Capture, publish, and consume stay as they are. See issue #8.

## Limits

- No snapshot of rows that already exist. Only later commits through `capture.DB` are replicated.
- No DDL, multi-row writes, joins, `RETURNING`, expressions, or primary-key changes.
- No `sql.Named` arguments and no prepared statements.
- Text values must be valid UTF-8. `time.Time` is stored as UTC.
- JetStream's duplicate window here is two minutes, which is the window this stream is configured with, not a server maximum. A republish after that window is a second message. Apply drops it when its sequence is older than the last applied change for that origin, table, and key.
- A unique violation that is not the primary key is a conflict: the message is terminated and logged, and later changes still apply. The row is not written.
- An update that matches nothing is upserted when the after-image includes the key. Otherwise the message is terminated and logged. The consumer does not stop.
- One publisher per capture is enough. Consumers use `MaxAckPending: 1`.
- The stream is a trust boundary for the data: a publisher can submit deletes, but identifiers are checked and values are bound parameters.
- SQLite must be a file database. A memory DSN would give each pooled connection its own empty database.

Not built: metrics, a CLI, logical decoding, and conflict handling beyond the sequence check and terminating non-primary-key unique violations.

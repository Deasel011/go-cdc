# go-cdc

`cdc` binds `database/sql` to NATS JetStream so a database can be replicated without tying the capture path to one engine's log.

Postgres and SQLite both speak `database/sql`. Postgres logical replication has no SQLite equivalent, so changes are captured in the client, inside the transaction that made them. The same path runs in either direction: Postgres to SQLite, and SQLite to Postgres.

## How it works

1. `Open` wraps a `pgx` or `sqlite` pool and creates a `cdc_outbox` table.
2. `ExecContext` / `Tx.Commit` run the write and insert one outbox row per change in that same transaction. A rollback publishes nothing. A crash after commit leaves the row for `Relay`.
3. `Relay` publishes each row to one JetStream subject, using the outbox message id as the JetStream message id, and deletes the row only after the server accepts it.
4. `Applier` reads that subject in order and applies the event with bound parameters. Inserts are `INSERT ... ON CONFLICT DO UPDATE`. Updates and deletes match the primary key.

Apply uses `DB.Std()` (the raw pool), so an applied row is not captured again. Each applier also ignores events whose origin is its own. Both directions can run on one stream without an echo loop.

`modernc.org/sqlite` is the SQLite driver because it is pure Go: `go test -race` does not need cgo. `mattn/go-sqlite3` is cgo and fights the race detector. `Open` appends `_busy_timeout=5000` and `_journal_mode=WAL` to a SQLite DSN that does not already set them, so the relay and the applier can share the file.

## Usage

```go
db, err := cdc.Open(ctx, cdc.Config{
    Driver:  cdc.DriverPostgres,
    DSN:     dsn,
    Origin:  "postgres",
    Subject: "cdc.events",
    Tables:  []cdc.Table{{Name: "items", Key: []string{"id"}}},
})
if err != nil {
    return err
}
defer db.Close()

if err := cdc.EnsureStream(ctx, js, "CDC", "cdc.events"); err != nil {
    return err
}
go db.Relay(ctx, js)

tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
if _, err := tx.ExecContext(ctx, `INSERT INTO items (id, name) VALUES ($1, $2)`, 1, "a"); err != nil {
    tx.Rollback()
    return err
}
if err := tx.Commit(); err != nil {
    return err
}
```

SQLite statements use `?`. The applier rewrites placeholders and identifier quoting for the target.

```go
ap, err := cdc.NewApplier(cdc.ApplyConfig{
    JS: js, Stream: "CDC", Subject: "cdc.events",
    Durable: "apply-on-sqlite", Target: sqlite.Std(),
    Driver: cdc.DriverSQLite, SkipOrigin: "sqlite",
})
go ap.Run(ctx)
```

Create the table on both sides yourself. Writes through `Std` are not replicated; use that for DDL.

## Statements that are captured

Single-row only, values as placeholders (`$1` or `?`, not mixed, each argument once):

```sql
INSERT INTO items (id, name) VALUES ($1, $2)
UPDATE items SET name = $1 WHERE id = $2
DELETE FROM items WHERE id = $1
```

`WHERE` must be equality on the primary key, combined with `AND` when the key has more than one column. Identifiers are unquoted and stored lowercase. A statement outside this subset fails the write and changes nothing.

## Limits

- No snapshot of rows that already exist. Only later commits through `DB` are replicated.
- No DDL, multi-row `INSERT`/`UPDATE`/`DELETE`, joins, `RETURNING`, expressions, or primary-key changes.
- No `sql.Named` arguments and no prepared statements.
- Text values must be valid UTF-8 (the event is JSON). `time.Time` is stored as UTC.
- The JetStream duplicate window is two minutes. A republish after that is a second message; applying it still converges because apply is idempotent.
- One relay per database is enough. Ordered apply uses `MaxAckPending: 1`.
- The stream is a trust boundary for the data: a publisher can submit deletes, but identifiers are checked against `[A-Za-z_][A-Za-z0-9_]*` and values are bound parameters, so a message cannot break out of the statement.
- SQLite must be a file database. A memory DSN would give each pooled connection its own empty database.

Not built: metrics, a CLI, logical decoding, and conflict handling other than ordered idempotent apply.

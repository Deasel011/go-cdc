package cdc

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver
	"github.com/nats-io/nats.go/jetstream"
	_ "modernc.org/sqlite" // pure-Go "sqlite" driver; go test -race needs no cgo
)

const outboxTable = "cdc_outbox"

// Drivers the package can capture and apply.
const (
	DriverPostgres = "pgx"
	DriverSQLite   = "sqlite"
)

// Op values carried by an event.
const (
	OpInsert = "insert"
	OpUpdate = "update"
	OpDelete = "delete"
)

// Table is a relation whose committed writes are captured.
// Key columns are the primary key, in index order.
type Table struct {
	Name string
	Key  []string
}

// Config opens a capturing database.
type Config struct {
	// Driver is DriverPostgres or DriverSQLite.
	Driver string
	DSN    string
	// Origin labels events from this database. An applier set to the same
	// origin ignores them, which stops an echo when both directions run.
	Origin string
	// Subject is the JetStream subject Relay publishes to.
	Subject string
	Tables  []Table
}

// Event is the JSON body published to JetStream.
// Values are typed strings so an int64 primary key is not rounded through JSON numbers.
type Event struct {
	MsgID  string  `json:"id"`
	Origin string  `json:"origin"`
	Op     string  `json:"op"`
	Table  string  `json:"table"`
	Key    []Field `json:"key"`
	After  []Field `json:"after,omitzero"`
}

// Field is one column in a key or an after-image.
type Field struct {
	Name  string `json:"name"`
	Value Value  `json:"value"`
}

// Value is one bound parameter. Data is omitted for null.
type Value struct {
	Type string `json:"type"`
	Data string `json:"data,omitzero"`
}

type tableMeta struct {
	name string
	key  []string
}

// DB captures committed writes. Reads and Std are not captured.
type DB struct {
	sql     *sql.DB
	driver  string
	origin  string
	subject string
	tables  map[string]tableMeta
	notify  chan struct{}
}

// Std returns the underlying pool. Writes on it are not captured and are not
// published, which is how schema changes and apply avoid an echo loop.
func (db *DB) Std() *sql.DB { return db.sql }

// Open pings the database and creates cdc_outbox when it is missing.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	if err := validateOrigin(cfg.Origin); err != nil {
		return nil, err
	}
	if cfg.Subject == "" {
		return nil, errors.New("cdc: subject is required")
	}
	if cfg.DSN == "" {
		return nil, errors.New("cdc: dsn is required")
	}
	if cfg.Driver != DriverPostgres && cfg.Driver != DriverSQLite {
		return nil, fmt.Errorf("cdc: unsupported driver %q", cfg.Driver)
	}
	tables, err := normalizeTables(cfg.Tables)
	if err != nil {
		return nil, err
	}
	dsn := cfg.DSN
	if cfg.Driver == DriverSQLite {
		dsn, err = sqliteDSN(dsn)
		if err != nil {
			return nil, err
		}
	}
	sqlDB, err := sql.Open(cfg.Driver, dsn)
	if err != nil {
		return nil, err
	}
	if cfg.Driver == DriverSQLite {
		// WAL allows the relay and the applier to use the file together.
		sqlDB.SetMaxOpenConns(8)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}
	db := &DB{
		sql:     sqlDB,
		driver:  cfg.Driver,
		origin:  cfg.Origin,
		subject: cfg.Subject,
		tables:  tables,
		notify:  make(chan struct{}, 1),
	}
	if err := db.createOutbox(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error { return db.sql.Close() }

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.sql.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.sql.QueryRowContext(ctx, query, args...)
}

// ExecContext commits one statement and, on success, its outbox row.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

// Tx is a transaction whose committed writes are captured.
// shortcut: no prepared statements; call ExecContext with the statement text.
type Tx struct {
	db      *DB
	tx      *sql.Tx
	ctx     context.Context
	changes []change
	done    bool
}

type change struct {
	op    string
	table string
	key   []Field
	after []Field
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.sql.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{db: db, tx: tx, ctx: ctx}, nil
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.tx.QueryRowContext(ctx, query, args...)
}

// ExecContext runs one captured statement. The change is published only if
// Commit succeeds, so a rollback leaves the stream alone.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx.done {
		return nil, errors.New("cdc: transaction finished")
	}
	parsed, err := parseDML(query, args)
	if err != nil {
		return nil, err
	}
	ch, err := tx.db.capture(parsed)
	if err != nil {
		return nil, err
	}
	res, err := tx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	tx.changes = append(tx.changes, ch)
	return snapshotResult(res), nil
}

// Commit writes one outbox row per change, then commits. The outbox insert is
// in the user transaction, so a crash before publish leaves the row to be
// retried and a failed commit publishes nothing.
func (tx *Tx) Commit() error {
	if tx.done {
		return sql.ErrTxDone
	}
	for _, ch := range tx.changes {
		id, err := newMsgID(tx.db.origin)
		if err != nil {
			_ = tx.tx.Rollback()
			tx.done = true
			return err
		}
		ev := Event{
			MsgID:  id,
			Origin: tx.db.origin,
			Op:     ch.op,
			Table:  ch.table,
			Key:    ch.key,
			After:  ch.after,
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			_ = tx.tx.Rollback()
			tx.done = true
			return err
		}
		if err := tx.insertOutbox(tx.ctx, id, payload); err != nil {
			_ = tx.tx.Rollback()
			tx.done = true
			return err
		}
	}
	if err := tx.tx.Commit(); err != nil {
		tx.done = true
		return err
	}
	tx.done = true
	if len(tx.changes) > 0 {
		tx.db.signal()
	}
	return nil
}

func (tx *Tx) Rollback() error {
	tx.done = true
	tx.changes = nil
	return tx.tx.Rollback()
}

func (tx *Tx) insertOutbox(ctx context.Context, id string, payload []byte) error {
	q := fmt.Sprintf(`INSERT INTO "cdc_outbox" ("msg_id", "payload") VALUES (%s, %s)`,
		placeholder(tx.db.driver, 1), placeholder(tx.db.driver, 2))
	_, err := tx.tx.ExecContext(ctx, q, id, payload)
	return err
}

func (db *DB) signal() {
	select {
	case db.notify <- struct{}{}:
	default:
	}
}

// Relay publishes outbox rows in id order until ctx is cancelled.
// A row is deleted only after JetStream accepts the publish. The message id
// is the outbox msg_id, so a retry inside the duplicate window is a no-op
// and a later retry still converges because apply is idempotent.
// shortcut: polls the outbox; use one relay per database. A second relay is safe inside the duplicate window.
func (db *DB) Relay(ctx context.Context, js jetstream.JetStream) error {
	if js == nil {
		return errors.New("cdc: nil jetstream")
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		n, err := db.relayBatch(ctx, js)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("cdc relay", "origin", db.origin, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-db.notify:
		case <-ticker.C:
		}
	}
}

func (db *DB) relayBatch(ctx context.Context, js jetstream.JetStream) (int, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT "id", "msg_id", "payload" FROM "cdc_outbox" ORDER BY "id" LIMIT 100`)
	if err != nil {
		return 0, err
	}
	type row struct {
		id      int64
		msgID   string
		payload []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.msgID, &r.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	for _, r := range batch {
		if _, err := js.Publish(ctx, db.subject, r.payload, jetstream.WithMsgID(r.msgID)); err != nil {
			return len(batch), err
		}
		q := fmt.Sprintf(`DELETE FROM "cdc_outbox" WHERE "id" = %s`, placeholder(db.driver, 1))
		if _, err := db.sql.ExecContext(ctx, q, r.id); err != nil {
			// The publish already landed. The next poll sends the same message
			// id and then retries the delete.
			return len(batch), err
		}
	}
	return len(batch), nil
}

func (db *DB) createOutbox(ctx context.Context) error {
	var ddl string
	switch db.driver {
	case DriverPostgres:
		ddl = `CREATE TABLE IF NOT EXISTS "cdc_outbox" (
			"id" BIGSERIAL PRIMARY KEY,
			"msg_id" TEXT NOT NULL UNIQUE,
			"payload" BYTEA NOT NULL
		)`
	case DriverSQLite:
		ddl = `CREATE TABLE IF NOT EXISTS "cdc_outbox" (
			"id" INTEGER PRIMARY KEY AUTOINCREMENT,
			"msg_id" TEXT NOT NULL UNIQUE,
			"payload" BLOB NOT NULL
		)`
	default:
		return fmt.Errorf("cdc: unsupported driver %q", db.driver)
	}
	_, err := db.sql.ExecContext(ctx, ddl)
	return err
}

func (db *DB) capture(p parsed) (change, error) {
	meta, ok := db.tables[p.table]
	if !ok {
		return change{}, fmt.Errorf("cdc: table %q is not registered", p.table)
	}
	switch p.op {
	case OpInsert:
		return captureInsert(meta, p)
	case OpUpdate:
		return captureUpdate(meta, p)
	case OpDelete:
		key, err := keyFields(meta, p.where, p.whereVals)
		if err != nil {
			return change{}, err
		}
		return change{op: OpDelete, table: meta.name, key: key}, nil
	default:
		return change{}, fmt.Errorf("cdc: unsupported op %q", p.op)
	}
}

func captureInsert(meta tableMeta, p parsed) (change, error) {
	if len(p.cols) == 0 {
		return change{}, errors.New("cdc: insert has no columns")
	}
	got := make(map[string]Value, len(p.cols))
	after := make([]Field, 0, len(p.cols))
	for i, col := range p.cols {
		if _, dup := got[col]; dup {
			return change{}, fmt.Errorf("cdc: duplicate column %q", col)
		}
		got[col] = p.colVals[i]
		after = append(after, Field{Name: col, Value: p.colVals[i]})
	}
	key := make([]Field, 0, len(meta.key))
	for _, k := range meta.key {
		v, ok := got[k]
		if !ok {
			return change{}, fmt.Errorf("cdc: table %q: insert must include primary key column %s", meta.name, k)
		}
		key = append(key, Field{Name: k, Value: v})
	}
	return change{op: OpInsert, table: meta.name, key: key, after: after}, nil
}

func captureUpdate(meta tableMeta, p parsed) (change, error) {
	if len(p.cols) == 0 {
		return change{}, errors.New("cdc: update has no columns")
	}
	key, err := keyFields(meta, p.where, p.whereVals)
	if err != nil {
		return change{}, err
	}
	keySet := make(map[string]bool, len(meta.key))
	for _, k := range meta.key {
		keySet[k] = true
	}
	seen := make(map[string]bool, len(p.cols))
	after := make([]Field, 0, len(p.cols))
	for i, col := range p.cols {
		if seen[col] {
			return change{}, fmt.Errorf("cdc: duplicate column %q", col)
		}
		seen[col] = true
		// shortcut: primary-key updates are unsupported; the key is the identity apply matches on.
		if keySet[col] {
			return change{}, fmt.Errorf("cdc: primary key updates are unsupported")
		}
		after = append(after, Field{Name: col, Value: p.colVals[i]})
	}
	return change{op: OpUpdate, table: meta.name, key: key, after: after}, nil
}

func keyFields(meta tableMeta, cols []string, vals []Value) ([]Field, error) {
	if len(cols) != len(vals) {
		return nil, errors.New("cdc: internal placeholder mismatch")
	}
	got := make(map[string]Value, len(cols))
	for i, col := range cols {
		if _, dup := got[col]; dup {
			return nil, fmt.Errorf("cdc: duplicate column %q", col)
		}
		got[col] = vals[i]
	}
	if len(got) != len(meta.key) {
		return nil, fmt.Errorf("cdc: table %q: WHERE must be the primary key (%s)", meta.name, strings.Join(meta.key, ", "))
	}
	out := make([]Field, 0, len(meta.key))
	for _, k := range meta.key {
		v, ok := got[k]
		if !ok {
			return nil, fmt.Errorf("cdc: table %q: WHERE must be the primary key (%s)", meta.name, strings.Join(meta.key, ", "))
		}
		out = append(out, Field{Name: k, Value: v})
	}
	return out, nil
}

var originRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,62}$`)

func validateOrigin(origin string) error {
	if !originRe.MatchString(origin) {
		return fmt.Errorf("cdc: invalid origin %q", origin)
	}
	return nil
}

func normalizeTables(tables []Table) (map[string]tableMeta, error) {
	if len(tables) == 0 {
		return nil, errors.New("cdc: at least one table is required")
	}
	out := make(map[string]tableMeta, len(tables))
	for _, table := range tables {
		name := strings.ToLower(table.Name)
		if _, err := quoteIdent(name); err != nil {
			return nil, err
		}
		if name == outboxTable {
			return nil, errors.New("cdc: cdc_outbox cannot be registered")
		}
		if len(table.Key) == 0 {
			return nil, fmt.Errorf("cdc: table %q: primary key is required", name)
		}
		key := make([]string, len(table.Key))
		seen := make(map[string]bool, len(table.Key))
		for i, col := range table.Key {
			col = strings.ToLower(col)
			if _, err := quoteIdent(col); err != nil {
				return nil, err
			}
			if seen[col] {
				return nil, fmt.Errorf("cdc: table %q: duplicate key column %q", name, col)
			}
			seen[col] = true
			key[i] = col
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("cdc: table %q registered twice", name)
		}
		out[name] = tableMeta{name: name, key: key}
	}
	return out, nil
}

func sqliteDSN(dsn string) (string, error) {
	lower := strings.ToLower(dsn)
	// Each connection to a memory database is empty, so the pool would not
	// share the outbox with the caller.
	if strings.Contains(lower, ":memory:") || strings.Contains(lower, "mode=memory") {
		return "", errors.New("cdc: sqlite DSN must be a file database so the pool shares one database")
	}
	if !strings.Contains(dsn, "_busy_timeout=") && !strings.Contains(dsn, "_timeout=") {
		dsn = appendQuery(dsn, "_busy_timeout=5000")
	}
	if !strings.Contains(dsn, "_journal_mode=") && !strings.Contains(dsn, "_journal=") {
		dsn = appendQuery(dsn, "_journal_mode=WAL")
	}
	return dsn, nil
}

func appendQuery(dsn, pair string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + pair
	}
	return dsn + "?" + pair
}

func newMsgID(origin string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return origin + "-" + hex.EncodeToString(b[:]), nil
}

type capturedResult struct {
	id    int64
	idErr error
	n     int64
	nErr  error
}

func (r capturedResult) LastInsertId() (int64, error) { return r.id, r.idErr }
func (r capturedResult) RowsAffected() (int64, error) { return r.n, r.nErr }

func snapshotResult(res sql.Result) sql.Result {
	out := capturedResult{}
	if res == nil {
		out.nErr = errors.New("cdc: nil result")
		out.idErr = out.nErr
		return out
	}
	out.n, out.nErr = res.RowsAffected()
	out.id, out.idErr = res.LastInsertId()
	return out
}

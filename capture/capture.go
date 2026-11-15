package capture

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Deasel011/go-cdc"
	"github.com/Deasel011/go-cdc/data"
)

const outboxTable = "cdc_outbox"

// Table is a relation whose committed writes are captured.
// Key columns are the primary key, in index order.
type Table struct {
	Name string
	Key  []string
}

// Config opens a capturing database.
type Config struct {
	DSN    string
	Origin string
	Tables []Table
}

// DB captures committed writes. Reads and Std are not captured.
// DB implements cdc.Capture: Changes reads the outbox in id order.
type DB struct {
	sql     *sql.DB
	dialect data.Dialect
	origin  string
	tables  map[string]tableMeta
	notify  chan struct{}
}

type tableMeta struct {
	name string
	key  []string
}

// Std returns the underlying pool. Writes on it are not captured.
func (db *DB) Std() *sql.DB { return db.sql }

// Open pings the database and creates cdc_outbox when it is missing.
func Open(ctx context.Context, dialect data.Dialect, cfg Config) (*DB, error) {
	if dialect == nil {
		return nil, errNilDialect
	}
	if err := validateOrigin(cfg.Origin); err != nil {
		return nil, err
	}
	if cfg.DSN == "" {
		return nil, ErrDSNRequired
	}
	tables, err := normalizeTables(cfg.Tables)
	if err != nil {
		return nil, err
	}
	dsn, err := dialect.PrepareDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	sqlDB, err := sql.Open(dialect.Driver(), dsn)
	if err != nil {
		return nil, err
	}
	dialect.Configure(sqlDB)
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, err
	}
	db := &DB{
		sql:     sqlDB,
		dialect: dialect,
		origin:  cfg.Origin,
		tables:  tables,
		notify:  make(chan struct{}, 1),
	}
	if _, err := db.sql.ExecContext(ctx, dialect.OutboxDDL()); err != nil {
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

// ExecContext commits one statement. A matching update or delete is captured.
// Zero rows captures nothing. More than one row returns ErrMultipleRows and
// rolls this statement's transaction back.
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
	mu      sync.Mutex
	changes []cdc.Change
	done    bool
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

// ExecContext runs one statement. An update or delete is captured only when
// it changes exactly one row. Zero rows is success with nothing to publish.
// More than one row returns ErrMultipleRows and does not record a change;
// rolling the transaction back is the caller's decision.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx.mu.Lock()
	done := tx.done
	tx.mu.Unlock()
	if done {
		return nil, ErrTransactionFinished
	}
	op, err := cdc.StatementOp(query)
	if err != nil {
		return nil, err
	}
	table, err := cdc.StatementTable(query)
	if err != nil {
		return nil, err
	}
	if _, ok := tx.db.tables[strings.ToLower(table)]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotRegistered, table)
	}
	encoded := make([]cdc.Value, len(args))
	for i, arg := range args {
		encoded[i], err = cdc.NewValue(arg)
		if err != nil {
			return nil, err
		}
	}
	res, err := tx.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	snap := snapshotResult(res)
	n, err := snap.RowsAffected()
	if err != nil {
		return nil, err
	}
	// A write that changed nothing is not a change. More than one row is
	// ambiguous to replay; the caller decides whether to roll back.
	if n == 0 {
		return snap, nil
	}
	if n > 1 {
		return nil, fmt.Errorf("%w: %d", ErrMultipleRows, n)
	}
	tx.mu.Lock()
	tx.changes = append(tx.changes, cdc.Change{Op: op, SQL: query, Args: encoded})
	tx.mu.Unlock()
	return snap, nil
}

// Commit writes one outbox row per captured change, then commits.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return ErrTransactionFinished
	}
	tx.done = true
	changes := append([]cdc.Change(nil), tx.changes...)
	tx.mu.Unlock()

	for i := range changes {
		if err := tx.insertChange(tx.ctx, &changes[i]); err != nil {
			_ = tx.tx.Rollback()
			return err
		}
	}
	if err := tx.tx.Commit(); err != nil {
		return err
	}
	if len(changes) > 0 {
		tx.db.signal()
	}
	return nil
}

func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	tx.done = true
	tx.changes = nil
	tx.mu.Unlock()
	return tx.tx.Rollback()
}

func (tx *Tx) insertChange(ctx context.Context, ch *cdc.Change) error {
	ch.Origin = tx.db.origin
	payload, err := json.Marshal(ch)
	if err != nil {
		return err
	}
	tmp, err := newMsgID(tx.db.origin)
	if err != nil {
		return err
	}
	q := fmt.Sprintf(`INSERT INTO "cdc_outbox" ("msg_id", "payload") VALUES (%s, %s) RETURNING "id"`,
		tx.db.dialect.Placeholder(1), tx.db.dialect.Placeholder(2))
	var id int64
	if err := tx.tx.QueryRowContext(ctx, q, tmp, payload).Scan(&id); err != nil {
		return err
	}
	ch.Seq = id
	ch.ID = fmt.Sprintf("%s-%d", tx.db.origin, id)
	payload, err = json.Marshal(ch)
	if err != nil {
		return err
	}
	q = fmt.Sprintf(`UPDATE "cdc_outbox" SET "msg_id" = %s, "payload" = %s WHERE "id" = %s`,
		tx.db.dialect.Placeholder(1), tx.db.dialect.Placeholder(2), tx.db.dialect.Placeholder(3))
	_, err = tx.tx.ExecContext(ctx, q, ch.ID, payload, id)
	return err
}

func (db *DB) signal() {
	select {
	case db.notify <- struct{}{}:
	default:
	}
}

// Changes reads the outbox in id order until ctx is cancelled.
// A row is deleted only after the consumer of the sequence returns, which is
// after jetstream.Publish has accepted the message. The JetStream message id
// is Change.ID, which is origin-seq and stable across retries.
// shortcut: polls the outbox; use one consumer of Changes per database.
func (db *DB) Changes(ctx context.Context) iter.Seq2[cdc.Change, error] {
	return func(yield func(cdc.Change, error) bool) {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			n, err := db.yieldBatch(ctx, yield)
			if errors.Is(err, errStop) {
				return
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				yield(cdc.Change{}, err)
				return
			}
			if n > 0 {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-db.notify:
			case <-ticker.C:
			}
		}
	}
}

func (db *DB) yieldBatch(ctx context.Context, yield func(cdc.Change, error) bool) (int, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT "id", "payload" FROM "cdc_outbox" ORDER BY "id" LIMIT 100`)
	if err != nil {
		return 0, err
	}
	type row struct {
		id      int64
		payload []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.payload); err != nil {
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
		var ch cdc.Change
		if err := json.Unmarshal(r.payload, &ch); err != nil {
			return len(batch), err
		}
		if ch.Seq == 0 {
			ch.Seq = r.id
		}
		if ch.ID == "" {
			ch.ID = fmt.Sprintf("%s-%d", db.origin, ch.Seq)
		}
		if !yield(ch, nil) {
			return 0, errStop
		}
		q := fmt.Sprintf(`DELETE FROM "cdc_outbox" WHERE "id" = %s`, db.dialect.Placeholder(1))
		if _, err := db.sql.ExecContext(ctx, q, r.id); err != nil {
			return len(batch), err
		}
	}
	return len(batch), nil
}

const (
	errNilDialect captureError = "capture: nil dialect"
	errStop       captureError = "capture: stop"
)

type captureError string

func (e captureError) Error() string { return string(e) }

var originRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,62}$`)

func validateOrigin(origin string) error {
	if !originRe.MatchString(origin) {
		return fmt.Errorf("%w: %q", ErrInvalidOrigin, origin)
	}
	return nil
}

func normalizeTables(tables []Table) (map[string]tableMeta, error) {
	if len(tables) == 0 {
		return nil, ErrNoTables
	}
	out := make(map[string]tableMeta, len(tables))
	for _, table := range tables {
		name := strings.ToLower(table.Name)
		if err := cdc.ValidIdent(name); err != nil {
			return nil, err
		}
		if name == outboxTable || name == "cdc_applied" {
			return nil, ErrOutboxTable
		}
		if len(table.Key) == 0 {
			return nil, fmt.Errorf("%w: %q", ErrPrimaryKeyRequired, name)
		}
		key := make([]string, len(table.Key))
		seen := make(map[string]bool, len(table.Key))
		for i, col := range table.Key {
			col = strings.ToLower(col)
			if err := cdc.ValidIdent(col); err != nil {
				return nil, err
			}
			if seen[col] {
				return nil, fmt.Errorf("%w: %q.%q", ErrDuplicateKeyColumn, name, col)
			}
			seen[col] = true
			key[i] = col
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateTable, name)
		}
		out[name] = tableMeta{name: name, key: key}
	}
	return out, nil
}

func newMsgID(origin string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return origin + "-tmp-" + hex.EncodeToString(b[:]), nil
}

type capturedResult struct {
	id    int64
	idErr error
	n     int64
	nErr  error
}

func (r capturedResult) LastInsertId() (int64, error) { return r.id, r.idErr }
func (r capturedResult) RowsAffected() (int64, error) { return r.n, r.nErr }

func snapshotResult(res sql.Result) capturedResult {
	out := capturedResult{}
	if res == nil {
		out.nErr = errNilResult
		out.idErr = out.nErr
		return out
	}
	out.n, out.nErr = res.RowsAffected()
	out.id, out.idErr = res.LastInsertId()
	return out
}

const errNilResult captureError = "capture: nil result"

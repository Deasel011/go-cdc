package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"modernc.org/sqlite"
)

// ApplyConfig consumes a stream and applies events to Target.
// Target should be the raw pool (DB.Std): writes there are not captured,
// so an applied row is not published again.
type ApplyConfig struct {
	JS         jetstream.JetStream
	Stream     string
	Subject    string
	Durable    string
	Target     *sql.DB
	Driver     string
	SkipOrigin string
}

// Applier applies events in stream order.
type Applier struct {
	js         jetstream.JetStream
	stream     string
	subject    string
	durable    string
	target     *sql.DB
	driver     string
	skipOrigin string
}

func NewApplier(cfg ApplyConfig) (*Applier, error) {
	if cfg.JS == nil || cfg.Target == nil {
		return nil, ErrApplierTarget
	}
	if cfg.Stream == "" || cfg.Subject == "" || cfg.Durable == "" || cfg.SkipOrigin == "" {
		return nil, ErrApplierConfig
	}
	if cfg.Driver != DriverPostgres && cfg.Driver != DriverSQLite {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDriver, cfg.Driver)
	}
	return &Applier{
		js:         cfg.JS,
		stream:     cfg.Stream,
		subject:    cfg.Subject,
		durable:    cfg.Durable,
		target:     cfg.Target,
		driver:     cfg.Driver,
		skipOrigin: cfg.SkipOrigin,
	}, nil
}

// Run applies events until ctx is cancelled. A permanent error (bad event or
// SQL that is not a lock conflict) is returned with the message left
// unacknowledged, so it is not dropped. Lock conflicts are retried.
func (a *Applier) Run(ctx context.Context) error {
	cons, err := a.js.CreateOrUpdateConsumer(ctx, a.stream, jetstream.ConsumerConfig{
		Durable:       a.durable,
		FilterSubject: a.subject,
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
		if err := a.handle(ctx, msg); err != nil {
			_ = msg.Nak()
			if ctx.Err() != nil {
				return nil
			}
			if _, ok := errors.AsType[permanentError](err); ok {
				return err
			}
			slog.Error("cdc apply", "durable", a.durable, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if err := msg.Ack(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (a *Applier) handle(ctx context.Context, msg jetstream.Msg) error {
	var ev Event
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		return permanent(fmt.Errorf("%w: %w", ErrDecodeEvent, err))
	}
	if err := ev.validate(); err != nil {
		return permanent(err)
	}
	if ev.Origin == a.skipOrigin {
		return nil
	}
	query, args, err := buildApply(a.driver, ev)
	if err != nil {
		return permanent(err)
	}
	res, err := a.exec(ctx, query, args)
	if err != nil {
		return err
	}
	if ev.Op != OpUpdate {
		return nil
	}
	n, err := res.RowsAffected()
	if err != nil {
		return permanent(err)
	}
	if n == 0 {
		return permanent(fmt.Errorf("%w: %s", ErrUpdateNoRows, ev.Table))
	}
	if n > 1 {
		return permanent(fmt.Errorf("%w: %s matched %d", ErrUpdateManyRows, ev.Table, n))
	}
	return nil
}

func (a *Applier) exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	var last error
	for attempt := range 6 {
		res, err := a.target.ExecContext(ctx, query, args...)
		if err == nil {
			return res, nil
		}
		last = err
		if !isBusy(err) {
			return nil, permanent(fmt.Errorf("%w: %s: %w", ErrApply, query, err))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	return nil, last
}

func (ev Event) validate() error {
	if ev.MsgID == "" || ev.Origin == "" {
		return ErrEventIdentity
	}
	switch ev.Op {
	case OpInsert, OpUpdate, OpDelete:
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedOp, ev.Op)
	}
	if _, err := quoteIdent(ev.Table); err != nil {
		return err
	}
	if ev.Table == outboxTable {
		return ErrApplyOutbox
	}
	if len(ev.Key) == 0 {
		return ErrEventKey
	}
	seenKey := make(map[string]bool, len(ev.Key))
	for _, f := range ev.Key {
		if err := checkField(f, seenKey); err != nil {
			return err
		}
	}
	if ev.Op == OpDelete {
		if len(ev.After) != 0 {
			return ErrDeleteAfter
		}
		return nil
	}
	if len(ev.After) == 0 {
		return ErrMissingAfter
	}
	seenAfter := make(map[string]bool, len(ev.After))
	for _, f := range ev.After {
		if err := checkField(f, seenAfter); err != nil {
			return err
		}
	}
	switch ev.Op {
	case OpInsert:
		for _, k := range ev.Key {
			if !seenAfter[k.Name] {
				return fmt.Errorf("%w: %s", ErrMissingKeyColumn, k.Name)
			}
		}
	case OpUpdate:
		for _, f := range ev.After {
			if seenKey[f.Name] {
				return ErrPrimaryKeyUpdate
			}
		}
	}
	return nil
}

func checkField(f Field, seen map[string]bool) error {
	if _, err := quoteIdent(f.Name); err != nil {
		return err
	}
	if seen[f.Name] {
		return fmt.Errorf("%w: %q", ErrDuplicateColumn, f.Name)
	}
	seen[f.Name] = true
	if _, err := f.Value.bind(); err != nil {
		return err
	}
	return nil
}

func buildApply(driver string, ev Event) (string, []any, error) {
	table, err := quoteIdent(ev.Table)
	if err != nil {
		return "", nil, err
	}
	switch ev.Op {
	case OpInsert:
		return buildInsert(driver, table, ev)
	case OpUpdate:
		return buildUpdate(driver, table, ev)
	case OpDelete:
		return buildDelete(driver, table, ev)
	default:
		return "", nil, fmt.Errorf("%w: %q", ErrUnsupportedOp, ev.Op)
	}
}

func buildInsert(driver, table string, ev Event) (string, []any, error) {
	cols := make([]string, len(ev.After))
	args := make([]any, len(ev.After))
	for i, f := range ev.After {
		cols[i] = f.Name
		v, err := f.Value.bind()
		if err != nil {
			return "", nil, err
		}
		args[i] = v
	}
	colSQL, err := quoteJoin(cols)
	if err != nil {
		return "", nil, err
	}
	ph := make([]string, len(args))
	for i := range args {
		ph[i] = placeholder(driver, i+1)
	}
	keys := make([]string, len(ev.Key))
	keySet := make(map[string]bool, len(ev.Key))
	for i, f := range ev.Key {
		keys[i] = f.Name
		keySet[f.Name] = true
	}
	keySQL, err := quoteJoin(keys)
	if err != nil {
		return "", nil, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) ", table, colSQL, strings.Join(ph, ", "), keySQL)
	var sets []string
	for _, name := range cols {
		if keySet[name] {
			continue
		}
		q, err := quoteIdent(name)
		if err != nil {
			return "", nil, err
		}
		sets = append(sets, q+" = EXCLUDED."+q)
	}
	if len(sets) == 0 {
		b.WriteString("DO NOTHING")
	} else {
		b.WriteString("DO UPDATE SET ")
		b.WriteString(strings.Join(sets, ", "))
	}
	return b.String(), args, nil
}

func buildUpdate(driver, table string, ev Event) (string, []any, error) {
	sets := make([]string, len(ev.After))
	args := make([]any, 0, len(ev.After)+len(ev.Key))
	for i, f := range ev.After {
		q, err := quoteIdent(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.bind()
		if err != nil {
			return "", nil, err
		}
		sets[i] = q + " = " + placeholder(driver, len(args)+1)
		args = append(args, v)
	}
	wheres := make([]string, 0, len(ev.Key))
	for _, f := range ev.Key {
		q, err := quoteIdent(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.bind()
		if err != nil {
			return "", nil, err
		}
		wheres = append(wheres, q+" = "+placeholder(driver, len(args)+1))
		args = append(args, v)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(sets, ", "), strings.Join(wheres, " AND ")), args, nil
}

func buildDelete(driver, table string, ev Event) (string, []any, error) {
	wheres := make([]string, len(ev.Key))
	args := make([]any, len(ev.Key))
	for i, f := range ev.Key {
		q, err := quoteIdent(f.Name)
		if err != nil {
			return "", nil, err
		}
		v, err := f.Value.bind()
		if err != nil {
			return "", nil, err
		}
		wheres[i] = q + " = " + placeholder(driver, i+1)
		args[i] = v
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(wheres, " AND ")), args, nil
}

func placeholder(driver string, n int) string {
	if driver == DriverPostgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func quoteIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("%w: %q", ErrInvalidIdentifier, name)
	}
	return `"` + name + `"`, nil
}

func quoteJoin(names []string) (string, error) {
	parts := make([]string, len(names))
	for i, name := range names {
		q, err := quoteIdent(name)
		if err != nil {
			return "", err
		}
		parts[i] = q
	}
	return strings.Join(parts, ", "), nil
}

// SQLite primary result codes. Retry these; do not ack the event away.
const (
	sqliteBusy   = 5
	sqliteLocked = 6
)

func isBusy(err error) bool {
	e, ok := errors.AsType[*sqlite.Error](err)
	if !ok {
		return false
	}
	code := e.Code() & 0xff
	return code == sqliteBusy || code == sqliteLocked
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{error: err}
}

// EnsureStream creates or updates the CDC stream.
// shortcut: the duplicate window is two minutes (the server default maximum). Redelivery after that still converges because apply is idempotent.
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

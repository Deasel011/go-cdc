package cdc_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Deasel011/go-cdc"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName  = "CDC"
	subjectName = "cdc.events"

	// wideID does not survive a JSON number (2^53+1). The codec has to keep it exact.
	wideID int64 = 9007199254740993

	// nastyNote would end the statement if apply concatenated values instead of binding them.
	nastyNote = "'); DROP TABLE items; --"
)

const pgItemsDDL = `CREATE TABLE items (
	id BIGINT PRIMARY KEY,
	name TEXT NOT NULL,
	qty BIGINT NOT NULL,
	note TEXT
)`

const sqliteItemsDDL = `CREATE TABLE items (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	qty INTEGER NOT NULL,
	note TEXT
)`

type item struct {
	ID   int64
	Name string
	Qty  int64
	Note sql.Null[string]
}

func text(s string) sql.Null[string] { return sql.Null[string]{V: s, Valid: true} }

func committedPair() []item {
	return []item{
		{ID: 2, Name: "gadget", Qty: 7, Note: text("new")},
		{ID: wideID, Name: "widget", Qty: 4, Note: text(nastyNote)},
	}
}

func afterPostgresDelete() []item {
	return []item{
		{ID: wideID, Name: "widget", Qty: 4, Note: text(nastyNote)},
	}
}

func afterSQLiteWrites() []item {
	return []item{
		{ID: 3, Name: "sprocket", Qty: 3},
		{ID: wideID, Name: "widget-v2", Qty: 4, Note: text(nastyNote)},
	}
}

type Step func(*Suite)

type Chain struct{ s *Suite }

func Given(step Step) *Chain {
	s := &Suite{errCh: make(chan error, 8)}
	step(s)
	return &Chain{s: s}
}

func (c *Chain) And(step Step) *Chain  { step(c.s); return c }
func (c *Chain) When(step Step) *Chain { return c.And(step) }
func (c *Chain) Then(step Step) *Chain { return c.And(step) }

type Suite struct {
	t        *testing.T
	pg       *cdc.DB
	lite     *cdc.DB
	js       jetstream.JetStream
	stream   string
	subject  string
	wg       sync.WaitGroup
	errCh    chan error
	rejected error
	raw      jetstream.RawStreamMsg
	ack      *jetstream.PubAck
	before   int
}

func (s *Suite) checkBackground() {
	s.t.Helper()
	select {
	case err := <-s.errCh:
		s.t.Fatalf("background: %v", err)
	default:
	}
}

func (s *Suite) background(fn func(context.Context) error) {
	s.wg.Go(func() {
		err := fn(s.t.Context())
		if err != nil && s.t.Context().Err() == nil {
			select {
			case s.errCh <- err:
			default:
			}
		}
	})
}

func APostgresDatabase(t *testing.T) Step {
	return func(s *Suite) {
		s.t = t
		ctx := t.Context()
		admin, err := sql.Open(cdc.DriverPostgres, postgresSocketDSN("postgres"))
		if err != nil {
			t.Fatal(err)
		}
		admin.SetMaxOpenConns(1)
		if err := admin.PingContext(ctx); err != nil {
			t.Fatalf("postgres: %v", err)
		}
		name := tempDBName()
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+pgQuote(name)); err != nil {
			t.Fatal(err)
		}
		db, err := cdc.Open(ctx, cdc.Config{
			Driver:  cdc.DriverPostgres,
			DSN:     postgresSocketDSN(name),
			Origin:  "postgres",
			Subject: subjectName,
			Tables:  []cdc.Table{{Name: "items", Key: []string{"id"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Std().ExecContext(ctx, pgItemsDDL); err != nil {
			t.Fatal(err)
		}
		s.pg = db
		t.Cleanup(func() {
			db.Close()
			dropCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := admin.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+pgQuote(name)+" WITH (FORCE)"); err != nil {
				t.Errorf("drop database: %v", err)
			}
			admin.Close()
		})
	}
}

func postgresSocketDSN(dbname string) string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("host=/var/run/postgresql user=%s dbname=%s sslmode=disable connect_timeout=5", u.Username, dbname)
}

func ASQLiteDatabase(t *testing.T) Step {
	return func(s *Suite) {
		s.t = t
		path := filepath.Join(t.TempDir(), "replica.db")
		db, err := cdc.Open(t.Context(), cdc.Config{
			Driver:  cdc.DriverSQLite,
			DSN:     "file:" + path,
			Origin:  "sqlite",
			Subject: subjectName,
			Tables:  []cdc.Table{{Name: "items", Key: []string{"id"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Std().ExecContext(t.Context(), sqliteItemsDDL); err != nil {
			t.Fatal(err)
		}
		s.lite = db
		t.Cleanup(func() { db.Close() })
	}
}

func AJetStream(t *testing.T) Step {
	return func(s *Suite) {
		s.t = t
		s.stream = streamName
		s.subject = subjectName
		opts := &server.Options{
			ServerName: t.Name(),
			Host:       "127.0.0.1",
			Port:       -1,
			NoLog:      true,
			NoSigs:     true,
			JetStream:  true,
			StoreDir:   t.TempDir(),
		}
		ns, err := server.NewServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		ns.Start()
		if !ns.ReadyForConnections(10 * time.Second) {
			t.Fatal("nats server not ready")
		}
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		if err := cdc.EnsureStream(t.Context(), js, s.stream, s.subject); err != nil {
			t.Fatal(err)
		}
		s.js = js
		t.Cleanup(func() {
			nc.Close()
			ns.Shutdown()
			ns.WaitForShutdown()
		})
	}
}

func ReplicationRunningBothWays(t *testing.T) Step {
	return func(s *Suite) {
		s.t = t
		start := func(db *cdc.DB, driver, durable, skip string) {
			ap, err := cdc.NewApplier(cdc.ApplyConfig{
				JS:         s.js,
				Stream:     s.stream,
				Subject:    s.subject,
				Durable:    durable,
				Target:     db.Std(),
				Driver:     driver,
				SkipOrigin: skip,
			})
			if err != nil {
				t.Fatal(err)
			}
			s.background(func(ctx context.Context) error { return db.Relay(ctx, s.js) })
			s.background(ap.Run)
		}
		start(s.pg, cdc.DriverPostgres, "apply-on-postgres", "postgres")
		start(s.lite, cdc.DriverSQLite, "apply-on-sqlite", "sqlite")
		t.Cleanup(func() {
			s.wg.Wait()
			for {
				select {
				case err := <-s.errCh:
					t.Error(err)
				default:
					return
				}
			}
		})
	}
}

func AnUnsupportedWriteIsAttempted(t *testing.T) Step {
	return func(s *Suite) {
		// A literal INSERT would succeed on Postgres if the wrapper executed it.
		_, err := s.pg.ExecContext(t.Context(), `INSERT INTO items (id, name, qty, note) VALUES (2, 'bad', 1, 'x')`)
		s.rejected = err
	}
}

func TheWriteIsRejectedAndNothingIsStored(t *testing.T) Step {
	return func(s *Suite) {
		if s.rejected == nil {
			t.Fatal("expected the literal insert to be rejected")
		}
		var n int64
		if err := s.pg.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM items`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("postgres items = %d", n)
		}
		if n, err := outboxCount(t.Context(), s.pg); err != nil || n != 0 {
			t.Fatalf("outbox = %d err=%v", n, err)
		}
		if got := len(s.messages()); got != 0 {
			t.Fatalf("stream messages = %d", got)
		}
	}
}

func PostgresCommitsInsertAndUpdate(t *testing.T) Step {
	return func(s *Suite) {
		ctx := t.Context()
		tx, err := s.pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES ($1, $2, $3, $4)`, wideID, "widget", 1, nastyNote); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE items SET qty = $1 WHERE id = $2`, 4, wideID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES ($1, $2, $3, $4)`, int64(2), "gadget", 7, "new"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func PostgresRollsBackADelete(t *testing.T) Step {
	return func(s *Suite) {
		ctx := t.Context()
		tx, err := s.pg.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `delete from items where id = $1`, int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func SQLiteShowsTheCommittedPostgresRows(t *testing.T) Step {
	return func(s *Suite) {
		s.waitBoth(committedPair())
	}
}

func TheRolledBackDeleteIsAbsentFromTheStream(t *testing.T) Step {
	return func(s *Suite) {
		s.checkBackground()
		got := s.events()
		want := []string{cdc.OpInsert, cdc.OpUpdate, cdc.OpInsert}
		if len(got) != len(want) {
			t.Fatalf("events = %d, want %d (%s)", len(got), len(want), opsOf(got))
		}
		for i, ev := range got {
			if ev.Origin != "postgres" || ev.Op != want[i] {
				t.Fatalf("event %d = %s %s, want postgres %s", i, ev.Origin, ev.Op, want[i])
			}
			if ev.Op == cdc.OpDelete {
				t.Fatalf("rolled-back delete was published: %+v", ev)
			}
		}
	}
}

func TheFirstEventIsRepublishedWithTheSameMsgID(t *testing.T) Step {
	return func(s *Suite) {
		msgs := s.messages()
		if len(msgs) == 0 {
			t.Fatal("no messages to republish")
		}
		s.before = len(msgs)
		s.raw = msgs[0]
		var ev cdc.Event
		if err := json.Unmarshal(s.raw.Data, &ev); err != nil {
			t.Fatal(err)
		}
		ack, err := s.js.Publish(t.Context(), s.subject, bytes.Clone(s.raw.Data), jetstream.WithMsgID(ev.MsgID))
		if err != nil {
			t.Fatal(err)
		}
		s.ack = ack
	}
}

func TheStreamDoesNotGrow(t *testing.T) Step {
	return func(s *Suite) {
		if s.ack == nil || !s.ack.Duplicate {
			t.Fatalf("republish ack = %+v, want duplicate", s.ack)
		}
		msgs := s.messages()
		if len(msgs) != s.before {
			t.Fatalf("stream messages = %d, want %d", len(msgs), s.before)
		}
		if msgs[0].Header.Get(jetstream.MsgIDHeader) == "" {
			t.Fatal("stored message has no Nats-Msg-Id header")
		}
	}
}

func PostgresCommitsADelete(t *testing.T) Step {
	return func(s *Suite) {
		if _, err := s.pg.ExecContext(t.Context(), `DELETE FROM items WHERE id = $1`, int64(2)); err != nil {
			t.Fatal(err)
		}
	}
}

func SQLiteNoLongerHasTheDeletedRow(t *testing.T) Step {
	return func(s *Suite) {
		s.waitBoth(afterPostgresDelete())
	}
}

func SQLiteIsWipedAndTheStreamIsReplayed(t *testing.T) Step {
	return func(s *Suite) {
		if _, err := s.lite.Std().ExecContext(t.Context(), `DELETE FROM items`); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := s.lite.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM items`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("wipe left %d rows", n)
		}
		ap, err := cdc.NewApplier(cdc.ApplyConfig{
			JS:         s.js,
			Stream:     s.stream,
			Subject:    s.subject,
			Durable:    "replay-on-sqlite",
			Target:     s.lite.Std(),
			Driver:     cdc.DriverSQLite,
			SkipOrigin: "sqlite",
		})
		if err != nil {
			t.Fatal(err)
		}
		s.background(ap.Run)
	}
}

func SQLiteMatchesTheCommittedPostgresState(t *testing.T) Step {
	return func(s *Suite) {
		s.waitRows(s.lite, afterPostgresDelete())
		s.waitRows(s.pg, afterPostgresDelete())
	}
}

func SQLiteCommitsAnUpdateAndInsert(t *testing.T) Step {
	return func(s *Suite) {
		ctx := t.Context()
		tx, err := s.lite.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE items SET name = ? WHERE id = ?`, "widget-v2", wideID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES (?, ?, ?, ?)`, int64(3), "sprocket", 3, nil); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func PostgresShowsTheSQLiteChanges(t *testing.T) Step {
	return func(s *Suite) {
		s.waitBoth(afterSQLiteWrites())
		var name string
		if err := s.pg.QueryRowContext(t.Context(), `SELECT name FROM items WHERE id = $1`, wideID).Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name != "widget-v2" {
			t.Fatalf("postgres name = %q", name)
		}
	}
}

func NoEchoWasPublished(t *testing.T) Step {
	return func(s *Suite) {
		s.checkBackground()
		got := s.events()
		want := []struct{ origin, op string }{
			{"postgres", cdc.OpInsert},
			{"postgres", cdc.OpUpdate},
			{"postgres", cdc.OpInsert},
			{"postgres", cdc.OpDelete},
			{"sqlite", cdc.OpUpdate},
			{"sqlite", cdc.OpInsert},
		}
		if len(got) != len(want) {
			t.Fatalf("events = %s, want 4 postgres and 2 sqlite", opsOf(got))
		}
		for i, ev := range got {
			if ev.Origin != want[i].origin || ev.Op != want[i].op {
				t.Fatalf("event %d = %s %s, want %s %s", i, ev.Origin, ev.Op, want[i].origin, want[i].op)
			}
		}
		again := s.events()
		if len(again) != len(got) {
			t.Fatalf("stream grew from %d to %d while idle", len(got), len(again))
		}
	}
}

func AMaliciousEventIsPublished(t *testing.T) Step {
	return func(s *Suite) {
		ev := cdc.Event{
			MsgID:  "evil-1",
			Origin: "attacker",
			Op:     cdc.OpDelete,
			Table:  `items"; DROP TABLE items; --`,
			Key: []cdc.Field{{
				Name:  "id",
				Value: cdc.Value{Type: "int", Data: "1"},
			}},
		}
		body, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.js.Publish(t.Context(), s.subject, body, jetstream.WithMsgID(ev.MsgID)); err != nil {
			t.Fatal(err)
		}
	}
}

func TheApplierStopsAndTheTableRemains(t *testing.T) Step {
	return func(s *Suite) {
		ap, err := cdc.NewApplier(cdc.ApplyConfig{
			JS:         s.js,
			Stream:     s.stream,
			Subject:    s.subject,
			Durable:    "reject-on-sqlite",
			Target:     s.lite.Std(),
			Driver:     cdc.DriverSQLite,
			SkipOrigin: "sqlite",
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		err = ap.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "invalid identifier") {
			t.Fatalf("applier error = %v", err)
		}
		var name string
		if err := s.lite.QueryRowContext(t.Context(), `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'`).Scan(&name); err != nil {
			t.Fatalf("items table: %v", err)
		}
		var n int64
		if err := s.lite.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM items`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("items has %d rows", n)
		}
	}
}

func (s *Suite) waitBoth(want []item) {
	s.t.Helper()
	s.waitRows(s.pg, want)
	s.waitRows(s.lite, want)
	s.waitOutbox(s.pg)
	s.waitOutbox(s.lite)
}

func (s *Suite) waitRows(db *cdc.DB, want []item) {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var last []item
	var lastErr error
	for {
		s.checkBackground()
		last, lastErr = readItems(s.t.Context(), db)
		if lastErr == nil && slices.Equal(last, want) {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("items = %#v (err %v), want %#v", last, lastErr, want)
		}
		select {
		case <-s.t.Context().Done():
			s.t.Fatal(s.t.Context().Err())
		case <-tick.C:
		}
	}
}

func (s *Suite) waitOutbox(db *cdc.DB) {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var last int64
	var lastErr error
	for {
		s.checkBackground()
		last, lastErr = outboxCount(s.t.Context(), db)
		if lastErr == nil && last == 0 {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("outbox = %d err=%v", last, lastErr)
		}
		select {
		case <-s.t.Context().Done():
			s.t.Fatal(s.t.Context().Err())
		case <-tick.C:
		}
	}
}

func (s *Suite) messages() []jetstream.RawStreamMsg {
	s.t.Helper()
	ctx := s.t.Context()
	st, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		s.t.Fatal(err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		s.t.Fatal(err)
	}
	if info.State.Msgs == 0 {
		return nil
	}
	out := make([]jetstream.RawStreamMsg, 0, info.State.Msgs)
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, err := st.GetMsg(ctx, seq)
		if err != nil {
			s.t.Fatalf("get msg %d: %v", seq, err)
		}
		msg.Data = bytes.Clone(msg.Data)
		out = append(out, *msg)
	}
	return out
}

func (s *Suite) events() []cdc.Event {
	s.t.Helper()
	msgs := s.messages()
	out := make([]cdc.Event, len(msgs))
	for i, msg := range msgs {
		if err := json.Unmarshal(msg.Data, &out[i]); err != nil {
			s.t.Fatalf("event %d: %v", msg.Sequence, err)
		}
	}
	return out
}

func readItems(ctx context.Context, db *cdc.DB) ([]item, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, name, qty, note FROM items ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.Name, &it.Qty, &it.Note); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func outboxCount(ctx context.Context, db *cdc.DB) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cdc_outbox`).Scan(&n)
	return n, err
}

func opsOf(events []cdc.Event) string {
	parts := make([]string, len(events))
	for i, ev := range events {
		parts[i] = ev.Origin + ":" + ev.Op
	}
	return strings.Join(parts, ",")
}

func tempDBName() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "cdc_" + hex.EncodeToString(b[:])
}

var pgNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func pgQuote(name string) string {
	if !pgNameRe.MatchString(name) {
		panic("bad database name " + name)
	}
	return `"` + name + `"`
}

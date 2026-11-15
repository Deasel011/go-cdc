package jetstream_test

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
	"time"

	"github.com/Deasel011/go-cdc"
	"github.com/Deasel011/go-cdc/capture"
	"github.com/Deasel011/go-cdc/data"
	"github.com/Deasel011/go-cdc/data/postgres"
	"github.com/Deasel011/go-cdc/data/sqlite"
	"github.com/Deasel011/go-cdc/stream/jetstream"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/suite"
)

const (
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

// ReplicationSuite is the integration fixture. SetupSuite owns the shared
// Postgres admin connection and the NATS server. SetupTest owns one Postgres
// database, one SQLite file, and one JetStream stream.
type ReplicationSuite struct {
	suite.Suite

	admin     *sql.DB
	ns        *server.Server
	nc        *nats.Conn
	js        natsjs.JetStream
	pgStore   *data.SQL
	liteStore *data.SQL

	ctx         context.Context
	cancel      context.CancelFunc
	pg          *capture.DB
	lite        *capture.DB
	pgName      string
	stream      string
	subject     string
	streamReady bool
	wg          sync.WaitGroup
	errCh       chan error

	rejected error
	raw      natsjs.RawStreamMsg
	ack      *natsjs.PubAck
	before   int
}

func (s *ReplicationSuite) SetupSuite() {
	s.T().Cleanup(s.shutdownShared)

	admin, err := sql.Open(postgres.Dialect{}.Driver(), postgresSocketDSN("postgres"))
	s.Require().NoError(err)
	admin.SetMaxOpenConns(1)
	s.Require().NoError(admin.PingContext(s.T().Context()))
	s.admin = admin

	opts := &server.Options{
		ServerName: "cdc-suite",
		Host:       "127.0.0.1",
		Port:       -1,
		NoLog:      true,
		NoSigs:     true,
		JetStream:  true,
		StoreDir:   s.T().TempDir(),
	}
	ns, err := server.NewServer(opts)
	s.Require().NoError(err)
	s.ns = ns
	ns.Start()
	s.Require().True(ns.ReadyForConnections(10*time.Second), "nats server not ready")
	nc, err := nats.Connect(ns.ClientURL())
	s.Require().NoError(err)
	s.nc = nc
	js, err := natsjs.New(nc)
	s.Require().NoError(err)
	s.js = js
}

func (s *ReplicationSuite) TearDownSuite() {
	s.shutdownShared()
}

func (s *ReplicationSuite) shutdownShared() {
	if s.nc != nil {
		s.nc.Close()
		s.nc = nil
	}
	if s.ns != nil {
		s.ns.Shutdown()
		s.ns.WaitForShutdown()
		s.ns = nil
	}
	if s.admin != nil {
		s.admin.Close()
		s.admin = nil
	}
}

func (s *ReplicationSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(s.T().Context())
	s.errCh = make(chan error, 8)
	s.rejected = nil
	s.ack = nil
	s.raw = natsjs.RawStreamMsg{}
	s.before = 0
	s.streamReady = false

	ctx := s.T().Context()
	s.pgName = tempDBName()
	s.stream, s.subject = streamFor(s.T().Name())
	_, err := s.admin.ExecContext(ctx, "CREATE DATABASE "+pgQuote(s.pgName))
	s.Require().NoError(err)

	s.Require().NoError(jetstream.EnsureStream(ctx, s.js, s.stream, s.subject))
	s.streamReady = true

	pg, err := capture.Open(ctx, postgres.Dialect{}, capture.Config{
		DSN:    postgresSocketDSN(s.pgName),
		Origin: "postgres",
		Tables: []capture.Table{{Name: "items", Key: []string{"id"}}},
	})
	s.Require().NoError(err)
	s.pg = pg
	_, err = pg.Std().ExecContext(ctx, pgItemsDDL)
	s.Require().NoError(err)
	s.pgStore, err = data.Open(ctx, pg.Std(), postgres.Dialect{})
	s.Require().NoError(err)

	lite, err := capture.Open(ctx, sqlite.Dialect{}, capture.Config{
		DSN:    "file:" + filepath.Join(s.T().TempDir(), "replica.db"),
		Origin: "sqlite",
		Tables: []capture.Table{{Name: "items", Key: []string{"id"}}},
	})
	s.Require().NoError(err)
	s.lite = lite
	_, err = lite.Std().ExecContext(ctx, sqliteItemsDDL)
	s.Require().NoError(err)
	s.liteStore, err = data.Open(ctx, lite.Std(), sqlite.Dialect{})
	s.Require().NoError(err)
}

func (s *ReplicationSuite) TearDownTest() {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.wg.Wait()
	if s.pg != nil {
		s.pg.Close()
		s.pg = nil
	}
	if s.lite != nil {
		s.lite.Close()
		s.lite = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if s.streamReady {
		s.Require().NoError(s.js.DeleteStream(ctx, s.stream))
		s.streamReady = false
	}
	if s.pgName != "" {
		_, err := s.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+pgQuote(s.pgName)+" WITH (FORCE)")
		s.Require().NoError(err)
		s.pgName = ""
	}
	for {
		select {
		case err := <-s.errCh:
			s.Require().NoError(err)
		default:
			return
		}
	}
}

func (s *ReplicationSuite) Given(step func()) *ReplicationSuite {
	step()
	return s
}

func (s *ReplicationSuite) And(step func()) *ReplicationSuite  { return s.Given(step) }
func (s *ReplicationSuite) When(step func()) *ReplicationSuite { return s.Given(step) }
func (s *ReplicationSuite) Then(step func()) *ReplicationSuite { return s.Given(step) }

func (s *ReplicationSuite) ReplicationRunningBothWays() {
	start := func(db *capture.DB, store *data.SQL, durable, skip string) {
		s.background(func(ctx context.Context) error {
			return jetstream.Publish(ctx, s.js, s.subject, db)
		})
		s.background(func(ctx context.Context) error {
			return jetstream.Consume(ctx, s.js, jetstream.ConsumeConfig{
				Stream:     s.stream,
				Subject:    s.subject,
				Durable:    durable,
				SkipOrigin: skip,
			}, store)
		})
	}
	start(s.pg, s.pgStore, "apply-on-postgres", "postgres")
	start(s.lite, s.liteStore, "apply-on-sqlite", "sqlite")
}

func (s *ReplicationSuite) AnInsertHasBeenPublished() {
	_, err := s.pg.ExecContext(s.T().Context(), `INSERT INTO items (id, name, qty, note) VALUES ($1, $2, $3, $4)`, wideID, "widget", 1, nastyNote)
	s.Require().NoError(err)
	s.waitOutbox(s.pg)
}

func (s *ReplicationSuite) BothSidesShowTheCommittedPair() {
	s.PostgresCommitsInsertAndUpdate()
	s.waitBoth(committedPair())
}

func (s *ReplicationSuite) BothSidesShowTheRowLeftAfterADelete() {
	s.BothSidesShowTheCommittedPair()
	s.PostgresCommitsADelete()
	s.waitBoth(afterPostgresDelete())
}

func (s *ReplicationSuite) AnUnsupportedWriteIsAttempted() {
	// database/sql would run this. Capture refuses it first, so the table stays.
	_, err := s.pg.ExecContext(s.T().Context(), `DROP TABLE items`)
	s.rejected = err
}

func (s *ReplicationSuite) TheWriteIsRejectedAndNothingIsStored() {
	s.Require().ErrorIs(s.rejected, cdc.ErrUnsupportedStatement)
	var n int64
	s.Require().NoError(s.pg.QueryRowContext(s.T().Context(), `SELECT COUNT(*) FROM items`).Scan(&n))
	s.Require().Zero(n)
	n, err := outboxCount(s.T().Context(), s.pg)
	s.Require().NoError(err)
	s.Require().Zero(n)
	s.Require().Empty(s.messages())
}

func (s *ReplicationSuite) PostgresCommitsInsertAndUpdate() {
	ctx := s.T().Context()
	tx, err := s.pg.BeginTx(ctx, nil)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES ($1, $2, $3, $4)`, wideID, "widget", 1, nastyNote)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `UPDATE items SET qty = $1 WHERE id = $2`, 4, wideID)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES ($1, $2, $3, $4)`, int64(2), "gadget", 7, "new")
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())
}

func (s *ReplicationSuite) PostgresRollsBackADelete() {
	ctx := s.T().Context()
	tx, err := s.pg.BeginTx(ctx, nil)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `delete from items where id = $1`, int64(2))
	s.Require().NoError(err)
	s.Require().NoError(tx.Rollback())
}

func (s *ReplicationSuite) SQLiteShowsTheCommittedPostgresRows() {
	s.waitBoth(committedPair())
}

func (s *ReplicationSuite) TheStreamHasOnlyTheCommittedPostgresChanges() {
	s.Require().NoError(s.backgroundErr())
	s.Require().Equal([]streamChange{
		{"postgres", cdc.Insert},
		{"postgres", cdc.Update},
		{"postgres", cdc.Insert},
	}, changes(s.events()))
}

func (s *ReplicationSuite) TheFirstEventIsRepublishedWithTheSameMsgID() {
	msgs := s.messages()
	s.Require().NotEmpty(msgs)
	s.before = len(msgs)
	s.raw = msgs[0]
	var ev cdc.Change
	s.Require().NoError(json.Unmarshal(s.raw.Data, &ev))
	ack, err := s.js.Publish(s.T().Context(), s.subject, bytes.Clone(s.raw.Data), natsjs.WithMsgID(ev.ID))
	s.Require().NoError(err)
	s.ack = ack
}

func (s *ReplicationSuite) TheStreamDoesNotGrow() {
	s.Require().NotNil(s.ack)
	s.Require().True(s.ack.Duplicate)
	msgs := s.messages()
	s.Require().Len(msgs, s.before)
	s.Require().NotEmpty(msgs[0].Header.Get(natsjs.MsgIDHeader))
}

func (s *ReplicationSuite) PostgresCommitsADelete() {
	_, err := s.pg.ExecContext(s.T().Context(), `DELETE FROM items WHERE id = $1`, int64(2))
	s.Require().NoError(err)
}

func (s *ReplicationSuite) SQLiteNoLongerHasTheDeletedRow() {
	s.waitBoth(afterPostgresDelete())
}

func (s *ReplicationSuite) SQLiteIsWipedAndTheStreamIsReplayed() {
	_, err := s.lite.Std().ExecContext(s.T().Context(), `DELETE FROM items`)
	s.Require().NoError(err)
	_, err = s.lite.Std().ExecContext(s.T().Context(), `DELETE FROM cdc_applied`)
	s.Require().NoError(err)
	var n int64
	s.Require().NoError(s.lite.QueryRowContext(s.T().Context(), `SELECT COUNT(*) FROM items`).Scan(&n))
	s.Require().Zero(n)
	s.background(func(ctx context.Context) error {
		return jetstream.Consume(ctx, s.js, jetstream.ConsumeConfig{
			Stream:     s.stream,
			Subject:    s.subject,
			Durable:    "replay-on-sqlite",
			SkipOrigin: "sqlite",
		}, s.liteStore)
	})
}

func (s *ReplicationSuite) SQLiteMatchesTheCommittedPostgresState() {
	s.waitRows(s.lite, afterPostgresDelete())
	s.waitRows(s.pg, afterPostgresDelete())
}

func (s *ReplicationSuite) SQLiteCommitsAnUpdateAndInsert() {
	ctx := s.T().Context()
	tx, err := s.lite.BeginTx(ctx, nil)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `UPDATE items SET name = ? WHERE id = ?`, "widget-v2", wideID)
	s.Require().NoError(err)
	_, err = tx.ExecContext(ctx, `INSERT INTO items (id, name, qty, note) VALUES (?, ?, ?, ?)`, int64(3), "sprocket", 3, nil)
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit())
}

func (s *ReplicationSuite) PostgresShowsTheSQLiteChanges() {
	s.waitBoth(afterSQLiteWrites())
}

func (s *ReplicationSuite) NoEchoWasPublished() {
	s.Require().NoError(s.backgroundErr())
	got := changes(s.events())
	want := []streamChange{
		{"postgres", cdc.Insert},
		{"postgres", cdc.Update},
		{"postgres", cdc.Insert},
		{"postgres", cdc.Delete},
		{"sqlite", cdc.Update},
		{"sqlite", cdc.Insert},
	}
	s.Require().Equal(want, got)
	s.Require().Len(changes(s.events()), len(got))
}

func (s *ReplicationSuite) AMaliciousEventIsPublished() {
	ev := cdc.Change{
		ID:     "evil-1",
		Origin: "attacker",
		Seq:    1,
		Op:     cdc.Delete,
		SQL:    `DROP TABLE items`,
	}
	body, err := json.Marshal(ev)
	s.Require().NoError(err)
	_, err = s.js.Publish(s.T().Context(), s.subject, body, natsjs.WithMsgID(ev.ID))
	s.Require().NoError(err)
}

func (s *ReplicationSuite) TheApplierStopsAndTheTableRemains() {
	// A bad change is terminated. The consumer keeps going, so a later insert lands.
	s.background(func(ctx context.Context) error {
		return jetstream.Consume(ctx, s.js, jetstream.ConsumeConfig{
			Stream:     s.stream,
			Subject:    s.subject,
			Durable:    "reject-on-sqlite",
			SkipOrigin: "sqlite",
		}, s.liteStore)
	})
	good := cdc.Change{
		ID: "good-1", Origin: "postgres", Seq: 2, Op: cdc.Insert,
		SQL: `INSERT INTO items (id, name, qty) VALUES (?, ?, ?)`,
		Args: []cdc.Value{
			{Type: "int", Data: "1"},
			{Type: "string", Data: "ok"},
			{Type: "int", Data: "1"},
		},
	}
	body, err := json.Marshal(good)
	s.Require().NoError(err)
	_, err = s.js.Publish(s.T().Context(), s.subject, body, natsjs.WithMsgID(good.ID))
	s.Require().NoError(err)
	s.waitRows(s.lite, []item{{ID: 1, Name: "ok", Qty: 1}})
	var name string
	s.Require().NoError(s.lite.QueryRowContext(s.T().Context(), `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'`).Scan(&name))
	s.Require().Equal("items", name)
}

func (s *ReplicationSuite) background(fn func(context.Context) error) {
	s.wg.Go(func() {
		err := fn(s.ctx)
		if err != nil && s.ctx.Err() == nil {
			select {
			case s.errCh <- err:
			default:
			}
		}
	})
}

func (s *ReplicationSuite) backgroundErr() error {
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}

func (s *ReplicationSuite) waitBoth(want []item) {
	s.T().Helper()
	s.waitRows(s.pg, want)
	s.waitRows(s.lite, want)
	s.waitOutbox(s.pg)
	s.waitOutbox(s.lite)
}

func (s *ReplicationSuite) waitRows(db *capture.DB, want []item) {
	s.T().Helper()
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var last []item
	var lastErr error
	for {
		s.Require().NoError(s.backgroundErr())
		last, lastErr = readItems(s.T().Context(), db)
		if lastErr == nil && slices.Equal(last, want) {
			return
		}
		if time.Now().After(deadline) {
			s.Require().NoError(lastErr)
			s.Require().Equal(want, last)
		}
		select {
		case <-s.T().Context().Done():
			s.Require().NoError(s.T().Context().Err())
		case <-tick.C:
		}
	}
}

func (s *ReplicationSuite) waitOutbox(db *capture.DB) {
	s.T().Helper()
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var last int64
	var lastErr error
	for {
		s.Require().NoError(s.backgroundErr())
		last, lastErr = outboxCount(s.T().Context(), db)
		if lastErr == nil && last == 0 {
			return
		}
		if time.Now().After(deadline) {
			s.Require().NoError(lastErr)
			s.Require().Zero(last)
		}
		select {
		case <-s.T().Context().Done():
			s.Require().NoError(s.T().Context().Err())
		case <-tick.C:
		}
	}
}

func (s *ReplicationSuite) messages() []natsjs.RawStreamMsg {
	s.T().Helper()
	ctx := s.T().Context()
	st, err := s.js.Stream(ctx, s.stream)
	s.Require().NoError(err)
	info, err := st.Info(ctx)
	s.Require().NoError(err)
	if info.State.Msgs == 0 {
		return nil
	}
	out := make([]natsjs.RawStreamMsg, 0, info.State.Msgs)
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, err := st.GetMsg(ctx, seq)
		s.Require().NoError(err)
		msg.Data = bytes.Clone(msg.Data)
		out = append(out, *msg)
	}
	return out
}

func (s *ReplicationSuite) events() []cdc.Change {
	s.T().Helper()
	msgs := s.messages()
	out := make([]cdc.Change, len(msgs))
	for i, msg := range msgs {
		s.Require().NoError(json.Unmarshal(msg.Data, &out[i]))
	}
	return out
}

type streamChange struct {
	Origin string
	Op     cdc.Op
}

func changes(events []cdc.Change) []streamChange {
	out := make([]streamChange, len(events))
	for i, ev := range events {
		out[i] = streamChange{Origin: ev.Origin, Op: ev.Op}
	}
	return out
}

func readItems(ctx context.Context, db *capture.DB) ([]item, error) {
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

func outboxCount(ctx context.Context, db *capture.DB) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cdc_outbox`).Scan(&n)
	return n, err
}

func tempDBName() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "cdc_" + hex.EncodeToString(b[:])
}

func streamFor(testName string) (string, string) {
	name := testName
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return "CDC_" + name, "cdc." + strings.ToLower(name)
}

func postgresSocketDSN(dbname string) string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("host=/var/run/postgresql user=%s dbname=%s sslmode=disable connect_timeout=5", u.Username, dbname)
}

var pgNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func pgQuote(name string) string {
	if !pgNameRe.MatchString(name) {
		panic("bad database name " + name)
	}
	return `"` + name + `"`
}

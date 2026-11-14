package data_test

import (
	"path/filepath"
	"testing"

	gosql "database/sql"
	"github.com/Deasel011/go-cdc"
	"github.com/Deasel011/go-cdc/data"
	"github.com/Deasel011/go-cdc/data/sqlite"
	"github.com/stretchr/testify/require"
)

func TestStaleSequenceDoesNotOverwrite(t *testing.T) {
	store, db := openStore(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	require.NoError(t, store.Apply(ctx, itemChange(2, cdc.Insert, "new")))
	require.NoError(t, store.Apply(ctx, itemChange(1, cdc.Update, "old")))
	var name string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name))
	require.Equal(t, "new", name)
}

func TestUpdateMissingRowUpsertsWhenImageIsComplete(t *testing.T) {
	store, db := openStore(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	ch := itemChange(1, cdc.Update, "full")
	ch.After = append(ch.After, ch.Key...)
	require.NoError(t, store.Apply(ctx, ch))
	var name string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name))
	require.Equal(t, "full", name)
}

func TestUpdateMissingRowDoesNotStickWhenImageIsPartial(t *testing.T) {
	store, db := openStore(t)
	_, err := db.ExecContext(t.Context(), `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	err = store.Apply(t.Context(), itemChange(1, cdc.Update, "partial"))
	require.ErrorIs(t, err, data.ErrIncompleteUpdate)
}

func TestUniqueConflictDoesNotBlockTheNextChange(t *testing.T) {
	store, db := openStore(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`)
	require.NoError(t, err)
	require.NoError(t, store.Apply(ctx, itemChange(1, cdc.Insert, "same")))
	err = store.Apply(ctx, namedChange(2, 2, "same"))
	require.ErrorIs(t, err, data.ErrConflict)
	require.NoError(t, store.Apply(ctx, namedChange(3, 3, "other")))
	var n int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&n))
	require.Equal(t, int64(2), n)
}

func openStore(t *testing.T) (*data.SQL, *gosql.DB) {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "apply.db")
	d, err := sqlite.Dialect{}.PrepareDSN(dsn)
	require.NoError(t, err)
	db, err := gosql.Open(sqlite.Dialect{}.Driver(), d)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	sqlite.Dialect{}.Configure(db)
	store, err := data.Open(t.Context(), db, sqlite.Dialect{})
	require.NoError(t, err)
	return store, db
}

func itemChange(seq int64, op cdc.Op, name string) cdc.Change {
	return namedChange(seq, 1, name, op)
}

func namedChange(seq, id int64, name string, op ...cdc.Op) cdc.Change {
	kind := cdc.Insert
	if len(op) == 1 {
		kind = op[0]
	}
	idVal := cdc.Value{Type: "int", Data: itoa(id)}
	ch := cdc.Change{
		ID: "t", Origin: "sqlite", Seq: seq, Op: kind, Table: "items",
		Key: []cdc.Field{{Name: "id", Value: idVal}},
		After: []cdc.Field{
			{Name: "id", Value: idVal},
			{Name: "name", Value: cdc.Value{Type: "string", Data: name}},
		},
	}
	if kind == cdc.Update {
		ch.After = []cdc.Field{{Name: "name", Value: cdc.Value{Type: "string", Data: name}}}
	}
	return ch
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

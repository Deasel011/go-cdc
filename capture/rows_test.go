package capture_test

import (
	"path/filepath"
	"testing"

	"github.com/Deasel011/go-cdc/capture"
	"github.com/Deasel011/go-cdc/data/sqlite"
	"github.com/stretchr/testify/require"
)

func TestUpdateMatchingNoRowsIsNotCaptured(t *testing.T) {
	db := openItems(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, `UPDATE items SET name = ? WHERE id = ?`, "ghost", int64(99))
	require.NoError(t, err)
	var n int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cdc_outbox`).Scan(&n))
	require.Zero(t, n)
}

func TestUpdateMatchingManyRowsErrors(t *testing.T) {
	ctx := t.Context()
	db, err := capture.Open(ctx, sqlite.Dialect{}, capture.Config{
		DSN:    "file:" + filepath.Join(t.TempDir(), "tags.db"),
		Origin: "sqlite",
		Tables: []capture.Table{{Name: "tags", Key: []string{"kind"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	_, err = db.Std().ExecContext(ctx, `CREATE TABLE tags (kind INTEGER, name TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO tags (kind, name) VALUES (?, ?)`, int64(1), "a")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO tags (kind, name) VALUES (?, ?)`, int64(1), "b")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE tags SET name = ? WHERE kind = ?`, "c", int64(1))
	require.ErrorIs(t, err, capture.ErrMultipleRows)
}

func openItems(t *testing.T) *capture.DB {
	t.Helper()
	ctx := t.Context()
	db, err := capture.Open(ctx, sqlite.Dialect{}, capture.Config{
		DSN:    "file:" + filepath.Join(t.TempDir(), "items.db"),
		Origin: "sqlite",
		Tables: []capture.Table{{Name: "items", Key: []string{"id"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	_, err = db.Std().ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER, note TEXT)`)
	require.NoError(t, err)
	return db
}

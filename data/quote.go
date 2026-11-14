package data

import (
	"fmt"
	"strings"

	"github.com/Deasel011/go-cdc"
)

// QuoteIdent double-quotes a validated identifier. Both Postgres and SQLite
// accept that form. An engine that does not can ignore this helper.
func QuoteIdent(name string) (string, error) {
	if err := cdc.ValidIdent(name); err != nil {
		return "", err
	}
	return `"` + name + `"`, nil
}

// QuoteJoin quotes names and joins them with commas.
func QuoteJoin(names []string) (string, error) {
	parts := make([]string, len(names))
	for i, name := range names {
		q, err := QuoteIdent(name)
		if err != nil {
			return "", err
		}
		parts[i] = q
	}
	return strings.Join(parts, ", "), nil
}

// Upsert builds INSERT ... ON CONFLICT DO UPDATE, which Postgres and SQLite share.
// placeholders are already rendered by the dialect and line up with cols.
func Upsert(table string, cols, keys, placeholders []string) (string, error) {
	t, err := QuoteIdent(table)
	if err != nil {
		return "", err
	}
	colSQL, err := QuoteJoin(cols)
	if err != nil {
		return "", err
	}
	keySQL, err := QuoteJoin(keys)
	if err != nil {
		return "", err
	}
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	var sets []string
	for _, name := range cols {
		if keySet[name] {
			continue
		}
		q, err := QuoteIdent(name)
		if err != nil {
			return "", err
		}
		sets = append(sets, q+" = EXCLUDED."+q)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) ", t, colSQL, strings.Join(placeholders, ", "), keySQL)
	if len(sets) == 0 {
		b.WriteString("DO NOTHING")
	} else {
		b.WriteString("DO UPDATE SET ")
		b.WriteString(strings.Join(sets, ", "))
	}
	return b.String(), nil
}

package data

import (
	"fmt"
	"strings"

	"github.com/Deasel011/go-cdc"
)

// Rebind rewrites $n or ? placeholders into the dialect's form and returns
// the arguments in the order the rewritten statement uses them.
// database/sql runs the result. This does not interpret the statement.
func Rebind(query string, d Dialect, args []cdc.Value) (string, []any, error) {
	var b strings.Builder
	out := make([]any, 0, len(args))
	dollar := false
	question := false
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'', '"':
			q := query[i]
			b.WriteByte(q)
			i++
			for i < len(query) && query[i] != q {
				b.WriteByte(query[i])
				i++
			}
			if i < len(query) {
				b.WriteByte(q)
			}
		case '?':
			question = true
			if dollar {
				return "", nil, cdc.ErrUnsupportedStatement
			}
			if len(out) >= len(args) {
				return "", nil, cdc.ErrUnsupportedStatement
			}
			v, err := args[len(out)].Bind()
			if err != nil {
				return "", nil, err
			}
			out = append(out, v)
			b.WriteString(d.Placeholder(len(out)))
		case '$':
			dollar = true
			if question || i+1 >= len(query) || query[i+1] < '1' || query[i+1] > '9' {
				b.WriteByte('$')
				continue
			}
			n := 0
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				n = n*10 + int(query[j]-'0')
				j++
			}
			if n < 1 || n > len(args) {
				return "", nil, fmt.Errorf("%w: $%d", cdc.ErrUnsupportedStatement, n)
			}
			v, err := args[n-1].Bind()
			if err != nil {
				return "", nil, err
			}
			out = append(out, v)
			b.WriteString(d.Placeholder(len(out)))
			i = j - 1
		default:
			b.WriteByte(query[i])
		}
	}
	return b.String(), out, nil
}

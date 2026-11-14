package cdc

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// shortcut: single-row INSERT, UPDATE, and DELETE only. Values are placeholders
// (? or $n, not mixed, each argument once). WHERE is equality on the primary
// key combined with AND. Upgrade when multi-row writes or other predicates must be captured.

type parsed struct {
	op        string
	table     string
	cols      []string
	nums      []int
	colVals   []Value
	where     []string
	whereNums []int
	whereVals []Value
}

func parseDML(sqlText string, args []any) (parsed, error) {
	toks, err := tokenize(sqlText)
	if err != nil {
		return parsed{}, err
	}
	p := &parser{toks: toks}
	kw, err := p.next()
	if err != nil {
		return parsed{}, err
	}
	if kw.kind != tokKeyword {
		return parsed{}, errors.New("cdc: unsupported statement")
	}
	var stmt parsed
	switch kw.text {
	case "insert":
		stmt, err = p.parseInsert()
	case "update":
		stmt, err = p.parseUpdate()
	case "delete":
		stmt, err = p.parseDelete()
	default:
		return parsed{}, errors.New("cdc: unsupported statement")
	}
	if err != nil {
		return parsed{}, err
	}
	if err := p.end(); err != nil {
		return parsed{}, err
	}
	if stmt.table == outboxTable {
		return parsed{}, errors.New("cdc: writes to cdc_outbox are not captured")
	}
	if err := stmt.bind(args); err != nil {
		return parsed{}, err
	}
	return stmt, nil
}

func (stmt *parsed) bind(args []any) error {
	nums := make([]int, 0, len(stmt.nums)+len(stmt.whereNums))
	nums = append(nums, stmt.nums...)
	nums = append(nums, stmt.whereNums...)
	vals, err := bindArgs(args, nums)
	if err != nil {
		return err
	}
	stmt.colVals = vals[:len(stmt.nums)]
	stmt.whereVals = vals[len(stmt.nums):]
	return nil
}

func bindArgs(args []any, nums []int) ([]Value, error) {
	seen := make(map[int]bool, len(nums))
	maxn := 0
	for _, n := range nums {
		if n < 1 {
			return nil, fmt.Errorf("cdc: invalid placeholder %d", n)
		}
		if seen[n] {
			return nil, fmt.Errorf("cdc: placeholder $%d reused", n)
		}
		seen[n] = true
		maxn = max(maxn, n)
	}
	if len(args) != maxn || len(seen) != len(args) {
		return nil, errors.New("cdc: placeholders must use each argument once")
	}
	out := make([]Value, len(nums))
	for i, n := range nums {
		v, err := encodeValue(args[n-1])
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func encodeValue(v any) (Value, error) {
	if _, ok := v.(sql.NamedArg); ok {
		// shortcut: sql.Named arguments are unsupported; pass positional args.
		return Value{}, errors.New("cdc: sql.Named arguments are unsupported")
	}
	dv, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return Value{}, fmt.Errorf("cdc: %w", err)
	}
	switch x := dv.(type) {
	case nil:
		return Value{Type: "null"}, nil
	case int64:
		return Value{Type: "int", Data: strconv.FormatInt(x, 10)}, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return Value{}, errors.New("cdc: non-finite float")
		}
		return Value{Type: "float", Data: strconv.FormatFloat(x, 'g', -1, 64)}, nil
	case bool:
		return Value{Type: "bool", Data: strconv.FormatBool(x)}, nil
	case string:
		return Value{Type: "string", Data: x}, nil
	case []byte:
		if x == nil {
			return Value{Type: "null"}, nil
		}
		return Value{Type: "bytes", Data: base64.StdEncoding.EncodeToString(x)}, nil
	case time.Time:
		return Value{Type: "time", Data: x.UTC().Format(time.RFC3339Nano)}, nil
	default:
		return Value{}, fmt.Errorf("cdc: unsupported value type %T", dv)
	}
}

func (v Value) bind() (any, error) {
	switch v.Type {
	case "null":
		if v.Data != "" {
			return nil, errors.New("cdc: null value has data")
		}
		return nil, nil
	case "int":
		n, err := strconv.ParseInt(v.Data, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cdc: int value: %w", err)
		}
		return n, nil
	case "float":
		n, err := strconv.ParseFloat(v.Data, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, fmt.Errorf("cdc: float value %q", v.Data)
		}
		return n, nil
	case "bool":
		n, err := strconv.ParseBool(v.Data)
		if err != nil {
			return nil, fmt.Errorf("cdc: bool value: %w", err)
		}
		return n, nil
	case "string":
		return v.Data, nil
	case "bytes":
		b, err := base64.StdEncoding.DecodeString(v.Data)
		if err != nil {
			return nil, fmt.Errorf("cdc: bytes value: %w", err)
		}
		return b, nil
	case "time":
		t, err := time.Parse(time.RFC3339Nano, v.Data)
		if err != nil {
			return nil, fmt.Errorf("cdc: time value: %w", err)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("cdc: unknown value type %q", v.Type)
	}
}

type tokKind int

const (
	tokIdent tokKind = iota
	tokKeyword
	tokPlaceholder
	tokComma
	tokLParen
	tokRParen
	tokEq
	tokSemi
)

type token struct {
	kind tokKind
	text string
	num  int
}

func tokenize(sqlText string) ([]token, error) {
	var toks []token
	qmark := 0
	dollar := false
	sawQmark := false
	i := 0
	for i < len(sqlText) {
		if isSpace(sqlText[i]) {
			i++
			continue
		}
		switch sqlText[i] {
		case ',':
			toks = append(toks, token{kind: tokComma})
			i++
		case '(':
			toks = append(toks, token{kind: tokLParen})
			i++
		case ')':
			toks = append(toks, token{kind: tokRParen})
			i++
		case '=':
			toks = append(toks, token{kind: tokEq})
			i++
		case ';':
			toks = append(toks, token{kind: tokSemi})
			i++
		case '?':
			sawQmark = true
			qmark++
			toks = append(toks, token{kind: tokPlaceholder, num: qmark})
			i++
		case '$':
			dollar = true
			n, next, err := scanDollar(sqlText, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokPlaceholder, num: n})
			i = next
		case '"':
			name, next, err := scanQuoted(sqlText, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokIdent, text: name})
			i = next
		default:
			if !isIdentStart(sqlText[i]) {
				return nil, fmt.Errorf("cdc: unsupported syntax at %q", sqlText[i:])
			}
			j := i + 1
			for j < len(sqlText) && isIdentCont(sqlText[j]) {
				j++
			}
			word := strings.ToLower(sqlText[i:j])
			if isKeyword(word) {
				toks = append(toks, token{kind: tokKeyword, text: word})
			} else {
				toks = append(toks, token{kind: tokIdent, text: word})
			}
			i = j
		}
	}
	if dollar && sawQmark {
		return nil, errors.New("cdc: mixed placeholder styles")
	}
	if len(toks) == 0 {
		return nil, errors.New("cdc: empty statement")
	}
	return toks, nil
}

func scanDollar(sqlText string, i int) (int, int, error) {
	j := i + 1
	if j >= len(sqlText) || sqlText[j] < '1' || sqlText[j] > '9' {
		return 0, 0, errors.New("cdc: invalid placeholder")
	}
	n := 0
	for j < len(sqlText) && sqlText[j] >= '0' && sqlText[j] <= '9' {
		n = n*10 + int(sqlText[j]-'0')
		j++
	}
	if n < 1 {
		return 0, 0, errors.New("cdc: invalid placeholder")
	}
	return n, j, nil
}

func scanQuoted(sqlText string, i int) (string, int, error) {
	j := i + 1
	for j < len(sqlText) && sqlText[j] != '"' {
		j++
	}
	if j >= len(sqlText) {
		return "", 0, errors.New("cdc: unterminated identifier")
	}
	name := strings.ToLower(sqlText[i+1 : j])
	if _, err := quoteIdent(name); err != nil {
		return "", 0, err
	}
	return name, j + 1, nil
}

func isKeyword(word string) bool {
	switch word {
	case "insert", "into", "values", "update", "set", "where", "and", "delete", "from":
		return true
	default:
		return false
	}
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r' || b == '\f' || b == '\v'
}

func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isIdentCont(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}

type parser struct {
	toks []token
	i    int
}

func (p *parser) peek() (token, bool) {
	if p.i >= len(p.toks) {
		return token{}, false
	}
	return p.toks[p.i], true
}

func (p *parser) next() (token, error) {
	t, ok := p.peek()
	if !ok {
		return token{}, errors.New("cdc: unexpected end of statement")
	}
	p.i++
	return t, nil
}

func (p *parser) keyword(want string) error {
	t, err := p.next()
	if err != nil {
		return err
	}
	if t.kind != tokKeyword || t.text != want {
		return fmt.Errorf("cdc: expected %s", want)
	}
	return nil
}

func (p *parser) ident() (string, error) {
	t, err := p.next()
	if err != nil {
		return "", err
	}
	if t.kind != tokIdent {
		return "", errors.New("cdc: expected identifier")
	}
	return t.text, nil
}

func (p *parser) ph() (int, error) {
	t, err := p.next()
	if err != nil {
		return 0, err
	}
	if t.kind != tokPlaceholder {
		return 0, errors.New("cdc: expected placeholder")
	}
	return t.num, nil
}

func (p *parser) end() error {
	t, ok := p.peek()
	if !ok {
		return nil
	}
	if t.kind == tokSemi {
		p.i++
		if _, ok := p.peek(); ok {
			return errors.New("cdc: unexpected trailing tokens")
		}
		return nil
	}
	return errors.New("cdc: unexpected trailing tokens")
}

func (p *parser) parseInsert() (parsed, error) {
	if err := p.keyword("into"); err != nil {
		return parsed{}, err
	}
	table, err := p.ident()
	if err != nil {
		return parsed{}, err
	}
	cols, err := p.identList()
	if err != nil {
		return parsed{}, err
	}
	if err := p.keyword("values"); err != nil {
		return parsed{}, err
	}
	nums, err := p.numList()
	if err != nil {
		return parsed{}, err
	}
	if len(cols) != len(nums) {
		return parsed{}, errors.New("cdc: column count does not match values")
	}
	return parsed{op: OpInsert, table: table, cols: cols, nums: nums}, nil
}

func (p *parser) parseUpdate() (parsed, error) {
	table, err := p.ident()
	if err != nil {
		return parsed{}, err
	}
	if err := p.keyword("set"); err != nil {
		return parsed{}, err
	}
	cols, nums, err := p.assignments()
	if err != nil {
		return parsed{}, err
	}
	where, whereNums, err := p.where()
	if err != nil {
		return parsed{}, err
	}
	return parsed{op: OpUpdate, table: table, cols: cols, nums: nums, where: where, whereNums: whereNums}, nil
}

func (p *parser) parseDelete() (parsed, error) {
	if err := p.keyword("from"); err != nil {
		return parsed{}, err
	}
	table, err := p.ident()
	if err != nil {
		return parsed{}, err
	}
	where, whereNums, err := p.where()
	if err != nil {
		return parsed{}, err
	}
	return parsed{op: OpDelete, table: table, where: where, whereNums: whereNums}, nil
}

func (p *parser) identList() ([]string, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var cols []string
	for {
		col, err := p.ident()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		t, err := p.next()
		if err != nil {
			return nil, err
		}
		switch t.kind {
		case tokRParen:
			return cols, nil
		case tokComma:
			continue
		default:
			return nil, errors.New("cdc: expected comma or closing parenthesis")
		}
	}
}

func (p *parser) numList() ([]int, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var nums []int
	for {
		n, err := p.ph()
		if err != nil {
			return nil, err
		}
		nums = append(nums, n)
		t, err := p.next()
		if err != nil {
			return nil, err
		}
		switch t.kind {
		case tokRParen:
			return nums, nil
		case tokComma:
			continue
		default:
			return nil, errors.New("cdc: expected comma or closing parenthesis")
		}
	}
}

func (p *parser) assignments() ([]string, []int, error) {
	var cols []string
	var nums []int
	for {
		col, err := p.ident()
		if err != nil {
			return nil, nil, err
		}
		if _, err := p.expect(tokEq); err != nil {
			return nil, nil, err
		}
		n, err := p.ph()
		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, col)
		nums = append(nums, n)
		t, ok := p.peek()
		if !ok || t.kind != tokComma {
			return cols, nums, nil
		}
		p.i++
	}
}

func (p *parser) where() ([]string, []int, error) {
	if err := p.keyword("where"); err != nil {
		return nil, nil, err
	}
	var cols []string
	var nums []int
	for {
		col, err := p.ident()
		if err != nil {
			return nil, nil, err
		}
		if _, err := p.expect(tokEq); err != nil {
			return nil, nil, err
		}
		n, err := p.ph()
		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, col)
		nums = append(nums, n)
		t, ok := p.peek()
		if !ok || t.kind != tokKeyword || t.text != "and" {
			return cols, nums, nil
		}
		p.i++
	}
}

func (p *parser) expect(kind tokKind) (token, error) {
	t, err := p.next()
	if err != nil {
		return token{}, err
	}
	if t.kind != kind {
		return token{}, errors.New("cdc: unexpected token")
	}
	return t, nil
}

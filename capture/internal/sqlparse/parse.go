package sqlparse

import (
	"fmt"
	"strings"

	"github.com/Deasel011/go-cdc"
)

// shortcut: single-row INSERT, UPDATE, and DELETE only. Values are placeholders
// (? or $n, not mixed, each argument once). WHERE is equality on the primary
// key combined with AND. Upgrade when multi-row writes or other predicates must be captured.

type parsed struct {
	op        cdc.Op
	table     string
	cols      []string
	nums      []int
	colVals   []cdc.Value
	where     []string
	whereNums []int
	whereVals []cdc.Value
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
		return parsed{}, errUnsupported
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
		return parsed{}, errUnsupported
	}
	if err != nil {
		return parsed{}, err
	}
	if err := p.end(); err != nil {
		return parsed{}, err
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

func bindArgs(args []any, nums []int) ([]cdc.Value, error) {
	seen := make(map[int]bool, len(nums))
	maxn := 0
	for _, n := range nums {
		if n < 1 {
			return nil, fmt.Errorf("%w: %d", errInvalidPlaceholder, n)
		}
		if seen[n] {
			return nil, fmt.Errorf("%w: $%d", errPlaceholderReused, n)
		}
		seen[n] = true
		maxn = max(maxn, n)
	}
	if len(args) != maxn || len(seen) != len(args) {
		return nil, errPlaceholderArity
	}
	out := make([]cdc.Value, len(nums))
	for i, n := range nums {
		v, err := cdc.NewValue(args[n-1])
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
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
				return nil, fmt.Errorf("%w: %q", errSyntax, sqlText[i:])
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
		return nil, errMixedPlaceholders
	}
	if len(toks) == 0 {
		return nil, errEmpty
	}
	return toks, nil
}

func scanDollar(sqlText string, i int) (int, int, error) {
	j := i + 1
	if j >= len(sqlText) || sqlText[j] < '1' || sqlText[j] > '9' {
		return 0, 0, errInvalidPlaceholder
	}
	n := 0
	for j < len(sqlText) && sqlText[j] >= '0' && sqlText[j] <= '9' {
		n = n*10 + int(sqlText[j]-'0')
		j++
	}
	if n < 1 {
		return 0, 0, errInvalidPlaceholder
	}
	return n, j, nil
}

func scanQuoted(sqlText string, i int) (string, int, error) {
	j := i + 1
	for j < len(sqlText) && sqlText[j] != '"' {
		j++
	}
	if j >= len(sqlText) {
		return "", 0, errUnterminatedIdent
	}
	name := strings.ToLower(sqlText[i+1 : j])
	if err := cdc.ValidIdent(name); err != nil {
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
		return token{}, errUnexpectedEnd
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
		return fmt.Errorf("%w: %s", errExpectedToken, want)
	}
	return nil
}

func (p *parser) ident() (string, error) {
	t, err := p.next()
	if err != nil {
		return "", err
	}
	if t.kind != tokIdent {
		return "", errExpectedToken
	}
	return t.text, nil
}

func (p *parser) ph() (int, error) {
	t, err := p.next()
	if err != nil {
		return 0, err
	}
	if t.kind != tokPlaceholder {
		return 0, errExpectedToken
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
			return errUnexpectedTrailing
		}
		return nil
	}
	return errUnexpectedTrailing
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
		return parsed{}, errColumnCount
	}
	return parsed{op: cdc.Insert, table: table, cols: cols, nums: nums}, nil
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
	return parsed{op: cdc.Update, table: table, cols: cols, nums: nums, where: where, whereNums: whereNums}, nil
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
	return parsed{op: cdc.Delete, table: table, where: where, whereNums: whereNums}, nil
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
			return nil, errExpectedToken
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
			return nil, errExpectedToken
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
		return token{}, errExpectedToken
	}
	return t, nil
}

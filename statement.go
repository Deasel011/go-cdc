package cdc

import "strings"

// StatementOp reports whether sql is one INSERT, UPDATE, or DELETE.
// database/sql already separates the statement from its arguments; this only
// refuses to replay anything that is not a single DML statement.
func StatementOp(sql string) (Op, error) {
	word, rest, err := firstWord(sql)
	if err != nil {
		return "", err
	}
	var op Op
	switch strings.ToLower(word) {
	case "insert":
		op = Insert
	case "update":
		op = Update
	case "delete":
		op = Delete
	default:
		return "", errWrap(ErrUnsupportedStatement, word)
	}
	if err := oneStatement(rest); err != nil {
		return "", err
	}
	return op, nil
}

// StatementTable returns the table named by a single INSERT, UPDATE, or DELETE.
// It does not interpret columns or expressions. database/sql still runs the statement.
func StatementTable(sql string) (string, error) {
	op, err := StatementOp(sql)
	if err != nil {
		return "", err
	}
	_, rest, err := firstWord(sql)
	if err != nil {
		return "", err
	}
	switch op {
	case Insert:
		word, rest, err := firstWord(rest)
		if err != nil || !strings.EqualFold(word, "into") {
			return "", ErrUnsupportedStatement
		}
		return leadingIdent(rest)
	case Update:
		return leadingIdent(rest)
	case Delete:
		word, rest, err := firstWord(rest)
		if err != nil || !strings.EqualFold(word, "from") {
			return "", ErrUnsupportedStatement
		}
		return leadingIdent(rest)
	default:
		return "", ErrUnsupportedStatement
	}
}

func leadingIdent(sql string) (string, error) {
	i := skipSpace(sql, 0)
	if i >= len(sql) {
		return "", ErrUnsupportedStatement
	}
	if sql[i] == '"' {
		j := strings.IndexByte(sql[i+1:], '"')
		if j < 0 {
			return "", ErrUnsupportedStatement
		}
		return sql[i+1 : i+1+j], nil
	}
	j := i
	for j < len(sql) && (isWord(sql[j]) || (sql[j] >= '0' && sql[j] <= '9')) {
		j++
	}
	if j == i {
		return "", ErrUnsupportedStatement
	}
	return sql[i:j], nil
}

func firstWord(sql string) (string, string, error) {
	i := skipSpace(sql, 0)
	if i >= len(sql) {
		return "", "", ErrUnsupportedStatement
	}
	j := i
	for j < len(sql) && isWord(sql[j]) {
		j++
	}
	if j == i {
		return "", "", ErrUnsupportedStatement
	}
	return sql[i:j], sql[j:], nil
}

// oneStatement rejects a second statement. A trailing semicolon is allowed.
// Semicolons inside quotes are part of the text, not a separator.
func oneStatement(sql string) error {
	sawSemi := false
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '\'', '"':
			q := sql[i]
			i++
			for i < len(sql) && sql[i] != q {
				if sql[i] == '\\' && q == '\'' && i+1 < len(sql) {
					i++
				}
				i++
			}
		case ';':
			sawSemi = true
		default:
			if sawSemi && !isSpace(sql[i]) {
				return ErrUnsupportedStatement
			}
		}
	}
	return nil
}

func skipSpace(s string, i int) int {
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	return i
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r' || b == '\f' || b == '\v'
}

func isWord(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

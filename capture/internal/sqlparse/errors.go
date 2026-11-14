package sqlparse

// errorString sentinels stay unexported. capture wraps them as ErrUnsupportedStatement.
type errorString string

func (e errorString) Error() string { return string(e) }

const (
	errUnsupported        errorString = "sqlparse: unsupported statement"
	errInvalidPlaceholder errorString = "sqlparse: invalid placeholder"
	errPlaceholderReused  errorString = "sqlparse: placeholder reused"
	errPlaceholderArity   errorString = "sqlparse: placeholders must use each argument once"
	errSyntax             errorString = "sqlparse: unsupported syntax"
	errMixedPlaceholders  errorString = "sqlparse: mixed placeholder styles"
	errEmpty              errorString = "sqlparse: empty statement"
	errUnterminatedIdent  errorString = "sqlparse: unterminated identifier"
	errUnexpectedEnd      errorString = "sqlparse: unexpected end of statement"
	errExpectedToken      errorString = "sqlparse: expected token"
	errUnexpectedTrailing errorString = "sqlparse: unexpected trailing tokens"
	errColumnCount        errorString = "sqlparse: column count does not match values"
)

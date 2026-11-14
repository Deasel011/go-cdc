package sqlparse

import "github.com/Deasel011/go-cdc"

// Statement is one parsed single-row write.
type Statement struct {
	Op        cdc.Op
	Table     string
	Cols      []string
	ColVals   []cdc.Value
	Where     []string
	WhereVals []cdc.Value
}

// Parse parses one INSERT, UPDATE, or DELETE in the supported subset.
func Parse(sqlText string, args []any) (Statement, error) {
	stmt, err := parseDML(sqlText, args)
	if err != nil {
		return Statement{}, err
	}
	return Statement{
		Op:        stmt.op,
		Table:     stmt.table,
		Cols:      stmt.cols,
		ColVals:   stmt.colVals,
		Where:     stmt.where,
		WhereVals: stmt.whereVals,
	}, nil
}

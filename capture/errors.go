package capture

// Error is a sentinel a caller of this package can act on.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrDSNRequired          Error = "capture: dsn is required"
	ErrInvalidOrigin        Error = "capture: invalid origin"
	ErrNoTables             Error = "capture: at least one table is required"
	ErrOutboxTable          Error = "capture: cdc_outbox cannot be registered"
	ErrPrimaryKeyRequired   Error = "capture: primary key is required"
	ErrDuplicateKeyColumn   Error = "capture: duplicate key column"
	ErrDuplicateTable       Error = "capture: table registered twice"
	ErrTableNotRegistered   Error = "capture: table is not registered"
	ErrWhereNotPrimaryKey   Error = "capture: WHERE must be the primary key"
	ErrNoColumns            Error = "capture: statement has no columns"
	ErrUnsupportedStatement Error = "capture: unsupported statement"
	ErrTransactionFinished  Error = "capture: transaction finished"
	// ErrMultipleRows means an update or delete matched more than one row.
	// The statement ran; the caller decides whether to roll the transaction back.
	ErrMultipleRows Error = "capture: statement matched multiple rows"
)

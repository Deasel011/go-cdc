package cdc

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"
)

// NewValue encodes a Go value into the wire form.
// shortcut: sql.Named arguments are unsupported; pass positional args.
func NewValue(v any) (Value, error) {
	if _, ok := v.(sql.NamedArg); ok {
		return Value{}, ErrNamedArg
	}
	dv, err := driver.DefaultParameterConverter.ConvertValue(v)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %w", ErrUnsupportedValue, err)
	}
	switch x := dv.(type) {
	case nil:
		return Value{Type: "null"}, nil
	case int64:
		return Value{Type: "int", Data: strconv.FormatInt(x, 10)}, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return Value{}, ErrNonFiniteFloat
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
		return Value{}, fmt.Errorf("%w: %T", ErrUnsupportedValue, dv)
	}
}

// Bind decodes the wire form into a database/sql argument.
func (v Value) Bind() (any, error) {
	switch v.Type {
	case "null":
		if v.Data != "" {
			return nil, ErrNullData
		}
		return nil, nil
	case "int":
		n, err := strconv.ParseInt(v.Data, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: int: %w", ErrBadValue, err)
		}
		return n, nil
	case "float":
		n, err := strconv.ParseFloat(v.Data, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, fmt.Errorf("%w: float %q", ErrBadValue, v.Data)
		}
		return n, nil
	case "bool":
		n, err := strconv.ParseBool(v.Data)
		if err != nil {
			return nil, fmt.Errorf("%w: bool: %w", ErrBadValue, err)
		}
		return n, nil
	case "string":
		return v.Data, nil
	case "bytes":
		b, err := base64.StdEncoding.DecodeString(v.Data)
		if err != nil {
			return nil, fmt.Errorf("%w: bytes: %w", ErrBadValue, err)
		}
		return b, nil
	case "time":
		t, err := time.Parse(time.RFC3339Nano, v.Data)
		if err != nil {
			return nil, fmt.Errorf("%w: time: %w", ErrBadValue, err)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownValueType, v.Type)
	}
}

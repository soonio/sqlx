package sqlx

import "errors"

var (
	ErrNotMatchDestination  = errors.New("not matching destination to scan")
	ErrNotReadableValue     = errors.New("value not addressable or interface")
	ErrNotSettable          = errors.New("passed in variable is not settable")
	ErrUnsupportedValueType = errors.New("unsupported unmarshal type")
)

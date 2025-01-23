package depoq

import (
	"database/sql"
	"errors"
)

var (
	// ErrNoRows is returned when no rows are found and the scan type is `ScanStruct` or `ScanMap`
	ErrNoRows = errors.New("no rows found")
)

// wrappedError is a map of errors that are wrapped by another error
var wrappedError map[error][]error = map[error][]error{
	ErrNoRows: {sql.ErrNoRows},
}

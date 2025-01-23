package result

import "io"

//go:generate mockgen --source=scanner.go --destination=scanner_mock.go --package=result
type Scanner interface {
	ScanStruct(dest interface{}) error
	ScanMap(dest map[string]interface{}) error
	ScanStructs(dest interface{}) error
	ScanMaps(dest *[]map[string]interface{}) error
	ScanWriter(dest io.Writer) error
	Close() error
}

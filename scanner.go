package depoq

// Scanner is a struct that contains the scanner
type Scanner struct {
	scannerType int
	dest        interface{}
}

const (
	noScanner = iota + 1
	scannerMap
	scannerMaps
	scannerStruct
	scannerStructs
	scannerWriter
)

// newScanner returns a new scanner
func newScanner(scannerType int, dest interface{}) *Scanner {

	return &Scanner{
		scannerType: scannerType,
		dest:        dest,
	}

}

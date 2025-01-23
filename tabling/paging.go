package tabling

type PagingType int

const (
	PagingTypeOffset PagingType = iota
	PagingTypeCursor
)

type Paging struct {
	PagingType PagingType
	Limit      int
	Offset     int
	Cursor     string
	ColumnID   string
}

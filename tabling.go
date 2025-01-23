package depoq

import (
	"depoq/tabling"
	"depoq/vars"
)

type Paging struct {
	tabling.Paging
}

// NewPagingOffset create new offset paging
func NewPagingOffset(limit, offset int) Paging {

	// set default limit
	if limit == 0 {
		limit = vars.DefaultPagingLimit
	}

	return Paging{
		tabling.Paging{
			PagingType: tabling.PagingTypeOffset,
			Limit:      limit,
			Offset:     offset,
		},
	}
}

// NewPagingCursor create new cursor paging
func NewPagingCursor(limit int, cursor string, columnID ...string) Paging {

	// set default limit
	if limit == 0 {
		limit = vars.DefaultPagingLimit
	}

	// set default columnID
	if len(columnID) == 0 {
		columnID = append(columnID, vars.DefaultPagingColumnID)
	}

	return Paging{
		tabling.Paging{
			PagingType: tabling.PagingTypeCursor,
			Cursor:     cursor,
			Limit:      limit,
			ColumnID:   columnID[0],
		},
	}
}

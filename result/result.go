package result

import (
	"database/sql"
	"net/http"
)

//go:generate mockgen --source=result.go --destination=result_mock.go --package result
type Result struct {
	Scanner  Scanner
	Metadata *Metadata
}

func Init(scanner Scanner, metadata *Metadata) *Result {

	return &Result{
		Scanner:  scanner,
		Metadata: metadata,
	}
}

type Metadata struct {
	RequestID string     `sika:"request_id"`
	SQLResult sql.Result `sika:"sql_result"`
	Paging    *Paging    `sika:"paging"`
	Columns   []string   `sika:"columns"`
}

type HTTPData struct {
	Header     http.Header `sika:"header" json:"header"`
	StatusCode int         `sika:"status_code" json:"status_code"`
	Body       []byte      `sika:"body" json:"body"`
}

type SQLResult struct {
	Result sql.Result `sika:"result"`
}

func (m *Metadata) HasSQLResult() bool {
	return m.SQLResult != nil
}

type PagingType string

const (
	PagingTypeCursor PagingType = "cursor"
	PagingTypeOffset PagingType = "offset"
)

type Paging struct {
	PagingType   PagingType    `sika:"paging_type"`
	PagingCursor *PagingCursor `sika:"paging_cursor"`
	PagingOffset *PagingOffset `sika:"paging_offset"`
}

type PagingCursor struct {
	Next  string `sika:"next"`
	Prev  string `sika:"prev"`
	Limit int    `sika:"limit"`
}

type PagingOffset struct {
	Limit     int `json:"limit"`
	Offset    int `json:"offset"`
	TotalData int `json:"total_data"`
}

type ExecResult struct {
	LastInsertID int64 `sika:"last_insert_id"`
	RowsAffected int64 `sika:"rows_affected"`
}

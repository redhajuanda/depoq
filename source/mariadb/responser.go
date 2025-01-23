package mariadb

import (
	"database/sql"
	"io"
	"reflect"

	tablingPkg "depoq/tabling"

	"depoq/result"
	"depoq/vars"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/pkg/errors"

	"github.com/jmoiron/sqlx"
)

type responser struct {
	rows            *sqlx.Rows
	res             sql.Result
	mapScanFunc     func(r rower, dest map[string]interface{}) error
	jsonMarshalFunc func(v interface{}) ([]byte, error)
	tabling         *Tabling
	meta            *result.Metadata
}

// extractResponseExec extracts the response from the exec
func (r *responser) extractResponseExec() error {

	if r.rows == nil {
		r.meta.SQLResult = r.res
	}

	return nil

}

// ScanStruct scans the first row of the result set into the provided struct
func (r *responser) ScanStruct(dest interface{}) error {

	if dest == nil {
		return errors.New("destination cannot be nil")
	}

	// Get the type of the provided value
	vType := reflect.TypeOf(dest)

	// Ensure that v is a pointer to a struct
	if vType.Kind() != reflect.Ptr || vType.Elem().Kind() != reflect.Struct {
		return errors.New("destination must be a pointer to a struct")
	}

	if r.rows == nil {
		return r.extractResponseExec()
	}
	defer r.rows.Close()

	// Initialize the dbscan API with the provided struct tag key and column separator
	api, err := dbscan.NewAPI(
		dbscan.WithStructTagKey(vars.TagKey),
		dbscan.WithColumnSeparator("__"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create new API")
	}

	// Scan one row into the struct, return an error if no rows are found and if the row is more than one
	err = api.ScanOne(dest, r.rows)
	if err != nil {
		if errors.Is(err, dbscan.ErrNotFound) {
			return sql.ErrNoRows
		}
		return errors.Wrap(err, "failed to scan struct")
	}

	return nil

}

// ScanMap scans the first row of the result set into the provided map
// The destination must be a map with string keys
func (r *responser) ScanMap(dest map[string]interface{}) error {

	if dest == nil {
		return errors.New("destination cannot be nil")
	}

	// extract the response exec if rows is nil
	if r.rows == nil {
		return r.extractResponseExec()
	}

	defer r.rows.Close()

	if !r.rows.Next() {
		return sql.ErrNoRows
	}

	// Use the mapScanFunc to scan the row into the map
	if err := r.mapScanFunc(r.rows, dest); err != nil {
		return err
	}

	return nil

}

// ScanStructs scans all rows of the result set into the provided slice of structs
// The destination must be a pointer to a slice of structs
func (r *responser) ScanStructs(dest interface{}) error {

	// Ensure v is a pointer to a slice
	sliceValue := reflect.ValueOf(dest)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		return errors.Errorf("destination must be a pointer to a slice of structs")
	}

	if r.rows == nil {
		return r.extractResponseExec()
	}
	defer r.rows.Close()

	// initialize the dbscan API with the provided struct tag key and column separator
	api, err := dbscan.NewAPI(
		dbscan.WithStructTagKey(vars.TagKey),
		dbscan.WithColumnSeparator("__"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create new API")
	}

	// Scan all rows into the slice of structs
	err = api.ScanAll(dest, r.rows)
	if err != nil {
		return errors.Wrap(err, "failed to scan structs")
	}

	return r.handleDataPagingStruct(dest)

}

// ScanMaps scans all rows of the result set into the provided slice of maps
// The destination must be a pointer to a slice of maps
func (r *responser) ScanMaps(dest *[]map[string]interface{}) error {

	if r.rows == nil {
		return r.extractResponseExec()
	}
	defer r.rows.Close()

	// loop through the rows and scan each row into a map
	for r.rows.Next() {

		// Initialize the map
		s := make(map[string]interface{})

		// Use the mapScanFunc to scan the row into the map
		err := r.mapScanFunc(r.rows, s)
		if err != nil {
			return err
		}

		// Append the scanned value to the slice
		*dest = append(*dest, s)

	}

	// handle data paging
	return r.handleDataPagingMap(dest)
}

// ScanWriter scans all rows of the result set into the provided writer
// The destination must be a writer
func (r *responser) ScanWriter(dest io.Writer) error {

	result := make([]map[string]interface{}, 0)

	// Scan the rows into a slice of maps
	err := r.ScanMaps(&result)
	if err != nil {
		return err
	}

	// Convert the result to JSON
	jsonResult, err := r.jsonMarshalFunc(result)
	if err != nil {
		return errors.Wrap(err, "failed to marshal result to JSON")
	}

	// Write the JSON to the writer
	_, err = dest.Write(jsonResult)
	if err != nil {
		return errors.Wrap(err, "failed to write JSON to writer")
	}

	return nil
}

// Close closes the rows
func (r *responser) Close() error {

	if r.rows != nil {
		return r.rows.Close()
	}

	return nil

}

// handleDataPaging handles the data paging
func (r *responser) handleDataPagingStruct(data interface{}) error {
	if r.tabling != nil && r.tabling.tabling != nil && r.tabling.tabling.Paging != nil {
		switch r.tabling.tabling.Paging.PagingType {
		case tablingPkg.PagingTypeOffset:
			totalData, err := r.tabling.handleDataOffsetStruct(data)
			if err != nil {
				return err
			}

			r.meta.Paging = &result.Paging{
				PagingType: result.PagingTypeOffset,
				PagingOffset: &result.PagingOffset{
					Offset:    r.tabling.tabling.Paging.Offset,
					Limit:     r.tabling.tabling.Paging.Limit,
					TotalData: totalData,
				},
			}

		case tablingPkg.PagingTypeCursor:
			next, prev, err := r.tabling.handleDataCursorStruct(data)
			if err != nil {
				return err
			}

			r.meta.Paging = &result.Paging{
				PagingType: result.PagingTypeCursor,
				PagingCursor: &result.PagingCursor{
					Next:  next,
					Prev:  prev,
					Limit: r.tabling.tabling.Paging.Limit,
				},
			}
		}
	}
	return nil

}

// handleDataPaging handles the data paging
func (r *responser) handleDataPagingMap(data *[]map[string]interface{}) error {

	err := r.tabling.sanitize(data)
	if err != nil {
		return err
	}

	if r.tabling != nil && r.tabling.tabling != nil && r.tabling.tabling.Paging != nil {
		switch r.tabling.tabling.Paging.PagingType {
		case tablingPkg.PagingTypeOffset:
			totalData, err := r.tabling.handleDataOffsetMap(data)
			if err != nil {
				return err
			}

			r.meta.Paging = &result.Paging{
				PagingType: result.PagingTypeOffset,
				PagingOffset: &result.PagingOffset{
					Offset:    r.tabling.tabling.Paging.Offset,
					Limit:     r.tabling.tabling.Paging.Limit,
					TotalData: totalData,
				},
			}
		case tablingPkg.PagingTypeCursor:
			next, prev, err := r.tabling.handleDataCursorMap(data)
			if err != nil {
				return err
			}

			r.meta.Paging = &result.Paging{
				PagingType: result.PagingTypeCursor,
				PagingCursor: &result.PagingCursor{
					Next:  next,
					Prev:  prev,
					Limit: r.tabling.tabling.Paging.Limit,
				},
			}

		}
	}

	return nil
}

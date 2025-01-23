package mariadb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	tablingPkg "depoq/tabling"
	"depoq/vars"

	"github.com/pkg/errors"
	"github.com/redhajuanda/sqlparser"
)

const (
	totalDataColumn = "total_data?h=0"
)

type Tabling struct {
	sql             string
	tabling         *tablingPkg.Tabling
	stmt            *sqlparser.Select
	cursorSeparator string
}

// NewTabling creates a new tabling instance.
func NewTabling(sql string, tabling *tablingPkg.Tabling) *Tabling {
	return &Tabling{
		sql:             sql,
		tabling:         tabling,
		cursorSeparator: "_--_",
	}
}

// Init initializes the tabling and returns the constructed sql query.
func (t *Tabling) Init() (string, error) {

	if t.tabling == nil {
		return "", nil
	}

	return t.init()
}

// init initializes the tabling and returns the constructed sql query.
func (t *Tabling) init() (string, error) {

	// create parser
	ps, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return "", errors.Wrap(err, "failed to create parser")
	}

	// parse sql query to sqlparser statement
	stmt, err := ps.Parse(t.sql)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse sql")
	}

	// handle the statement based on the type
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		t.stmt = stmt
		return t.handleSelectStmt(stmt)
	default:
		return "", errors.New("unsupported statement type")
	}

}

// handleSelectStmt handles the select statement.
func (t *Tabling) handleSelectStmt(stmt *sqlparser.Select) (string, error) {

	var (
		result    string
		paging    *tablingPkg.Paging  = t.tabling.Paging
		sorting   *tablingPkg.Sorting = t.tabling.Sorting
		tableName string              = strings.ReplaceAll(strings.Split(sqlparser.String(stmt.GetFrom()[0]), " ")[0], "`", "") // get main table name
	)

	if paging != nil { // handle paging if exists
		switch paging.PagingType {

		case tablingPkg.PagingTypeOffset: // handle offset based pagination
			err := t.handlePagingOffset(tableName)
			if err != nil {
				return result, err
			}
		case tablingPkg.PagingTypeCursor:
			err := t.handlePagingCursor()
			if err != nil {
				return result, err
			}
		default:
			return result, errors.New("unsupported paging type")
		}
	} else if sorting != nil { // handle sorting if exists
		err := t.setSorting()
		if err != nil {
			return result, err
		}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.Format(buf)
	result = buf.String()

	// replace bind variables
	return replaceBindVariables(result), nil
}

// handlePagingOffset handles the offset limit pagination.
func (t *Tabling) handlePagingOffset(qualifier string) error {
	// construct count over
	countOver := sqlparser.String(&sqlparser.FuncExpr{
		Name: sqlparser.NewIdentifierCI("COUNT"),
		Exprs: sqlparser.Exprs{
			&sqlparser.ColName{
				Name:      sqlparser.NewIdentifierCI("id"),
				Qualifier: sqlparser.NewTableName(qualifier),
			},
		},
	})
	countOver += " OVER()"

	// add count total data to the select statement
	t.stmt.AddSelectExprs([]sqlparser.SelectExpr{
		&sqlparser.AliasedExpr{
			Expr: sqlparser.NewBitLiteral(countOver),
			As:   sqlparser.NewIdentifierCI(totalDataColumn),
		},
	})

	// set limit and offset
	t.stmt.SetLimit(&sqlparser.Limit{
		Offset:   sqlparser.NewIntLiteral(fmt.Sprintf("%d", t.tabling.Paging.Offset)),
		Rowcount: sqlparser.NewIntLiteral(fmt.Sprintf("%d", t.tabling.Paging.Limit)),
	})

	// set sorting
	return t.setSorting()

}

// handlePagingCursor handles the cursor pagination.
func (t *Tabling) handlePagingCursor() error {

	var (
		limit  = t.tabling.Paging.Limit
		sort   = t.tabling.Sorting.Sort
		cursor = t.tabling.Paging.Cursor
	)

	if cursor == "" { // handle first page
		return t.cursorSetLimitAndSort(limit, t.cursorPrepareSort(sort, false))
	}

	// set where clause
	changeOrderDirection, err := t.cursorSetWhere()
	if err != nil {
		return err
	}

	// set limit and sort
	return t.cursorSetLimitAndSort(limit, t.cursorPrepareSort(sort, changeOrderDirection))

}

// cursorPrepareSort prepares the sort for the cursor pagination.
func (t *Tabling) cursorPrepareSort(sort string, changeOrderDirection bool) []string {

	var (
		sortType string
		sorts    = make([]string, 0)
	)

	if changeOrderDirection {
		if strings.HasPrefix(sort, "-") {
			sort = "+" + strings.TrimPrefix(sort, "-")
			sortType = "+"
		} else {
			sort = "-" + strings.TrimPrefix(sort, "+")
			sortType = "-"
		}
	}

	sorts = append(sorts, sort)
	sorts = append(sorts, fmt.Sprintf("%s%s", sortType, "id"))

	return sorts
}

// cursorSetLimitAndSort sets the limit and sort for the cursor pagination.
func (t *Tabling) cursorSetLimitAndSort(limit int, sort []string) error {

	// set limit to limit + 1
	t.stmt.SetLimit(&sqlparser.Limit{
		Rowcount: sqlparser.NewIntLiteral(fmt.Sprintf("%d", limit+1)),
	})

	// set sorting for column
	for _, s := range sort {
		err := t.addSorting(&tablingPkg.Sorting{Sort: s})
		if err != nil {
			return err
		}
	}

	return nil
}

// cursorSetWhere sets the where clause for the cursor pagination.
func (t *Tabling) cursorSetWhere() (changeSortDirection bool, err error) {

	var (
		cursor           = t.tabling.Paging.Cursor
		sort             = t.tabling.Sorting.Sort
		sortBy, sortType = t.parseSort(sort)
		cursorPrefix     string
		cursorCol        string
		cursorID         string
	)

	// parse cursor
	cursorPrefix, cursorCol, cursorID, err = t.parseCursor(cursor)
	if err != nil {
		return false, err
	}

	// get select columns
	_, columnSanitized, columnNullables, err := t.getSelectColumns()
	if err != nil {
		return false, errors.Wrap(err, "failed to get select columns")
	}

	var left sqlparser.ValTuple

	if columnNullables[sortBy] {
		left = sqlparser.ValTuple{
			&sqlparser.FuncExpr{
				Name: sqlparser.NewIdentifierCI("COALESCE"),
				Exprs: sqlparser.Exprs{
					&sqlparser.ColName{
						Name: sqlparser.NewIdentifierCI(columnSanitized[sortBy]),
					},
					sqlparser.NewStrLiteral(""),
				},
			},
			&sqlparser.ColName{
				// Name: sqlparser.NewColIdent("id"),
				Name: sqlparser.NewIdentifierCI("id"),
			},
		}
	} else {
		if len(strings.Split(columnSanitized[sortBy], ".")) == 2 {
			left = sqlparser.ValTuple{
				&sqlparser.ColName{
					Qualifier: sqlparser.NewTableName(strings.Split(columnSanitized[sortBy], ".")[0]),
					Name:      sqlparser.NewIdentifierCI(strings.Split(columnSanitized[sortBy], ".")[1]),
				},
				&sqlparser.ColName{
					// Name: sqlparser.NewColIdent("id"),
					Qualifier: sqlparser.NewTableName(strings.Split(columnSanitized[sortBy], ".")[0]),
					Name:      sqlparser.NewIdentifierCI("id"),
				},
			}
		} else {
			left = sqlparser.ValTuple{
				&sqlparser.ColName{
					Name: sqlparser.NewIdentifierCI(strings.Split(columnSanitized[sortBy], ".")[0]),
				},
				&sqlparser.ColName{
					Name: sqlparser.NewIdentifierCI("id"),
				},
			}
		}
	}

	var right sqlparser.ValTuple

	if columnNullables[sortBy] && cursorCol == "<nil>" {
		right = sqlparser.ValTuple{
			&sqlparser.FuncExpr{
				Name: sqlparser.NewIdentifierCI("COALESCE"),
				Exprs: sqlparser.Exprs{
					sqlparser.NewBitLiteral("NULL"),
					sqlparser.NewStrLiteral(""),
				},
			},
			sqlparser.NewStrLiteral(cursorID),
		}
	} else {

		right = sqlparser.ValTuple{
			sqlparser.NewStrLiteral(cursorCol),
			sqlparser.NewStrLiteral(cursorID),
		}
	}

	operator, changeOrderDirection, _ := getOperator(cursorPrefix, sortType)

	t.stmt.AddWhere(&sqlparser.ComparisonExpr{
		Operator: operator,
		Left:     left,
		Right:    right,
	})

	return changeOrderDirection, nil
}

// parseCursor parses the cursor.
func (t *Tabling) parseCursor(cursor string) (prefix string, col string, id string, err error) {

	// decode cursor from base64
	decodedCursor, err := t.decodeCursor(cursor)
	if err != nil {
		return
	}

	cursorSplit := strings.Split(decodedCursor, t.cursorSeparator)
	if len(cursorSplit) != 3 {
		err = fmt.Errorf("cursor format is invalid")
		return
	}

	prefix = cursorSplit[0]
	col = cursorSplit[2]
	id = cursorSplit[1]

	return
}

// encodeCursor encodes the cursor.
func (t *Tabling) encodeCursor(cursor string) string {
	return base64.StdEncoding.EncodeToString([]byte(cursor))
}

// decodeCursor decodes the cursor.
func (t *Tabling) decodeCursor(cursor string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

// getSlectColumns returns the select columns from the statement.
func (t *Tabling) getSelectColumns() (columnOriginal map[string]string, columnSanitized map[string]string, columnNullables map[string]bool, err error) {

	var (
		columns = t.stmt.SelectExprs
	)

	columnOriginal = make(map[string]string)
	columnSanitized = make(map[string]string)
	columnNullables = make(map[string]bool)

	for _, col := range columns {
		switch expr := col.(type) {
		case *sqlparser.AliasedExpr:

			var alias = expr.As.String()
			var colName string
			if alias == "" {
				alias = expr.Expr.(*sqlparser.ColName).Name.String()
			}

			if col, ok := expr.Expr.(*sqlparser.ColName); ok {
				if col.Qualifier.Name.String() != "" {
					colName = fmt.Sprintf("%s.%s", col.Qualifier.Name.String(), col.Name.String())
				} else {
					colName = col.Name.String()
				}
			}
			columnOriginal[alias] = colName

		case *sqlparser.StarExpr:
			err = fmt.Errorf("star expression is not supported")
			return
		}
	}

	for i := range columnOriginal {
		val, err := url.Parse(i)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to parse query")
		}

		columnSanitized[val.Path] = columnOriginal[i]
		columnNullables[val.Path] = val.Query().Get("nullable") == "true"
	}

	return

}

// getOperator gets the operator for the cursor pagination.
func getOperator(prefix string, orderType string) (sqlparser.ComparisonExprOperator, bool, sqlparser.OrderDirection) {

	var (
		next            = prefix == "next"
		prev            = !next
		operator        sqlparser.ComparisonExprOperator
		orderDirection  sqlparser.OrderDirection
		changeDirection bool
	)

	if next && orderType == "-" {
		operator = sqlparser.LessThanOp
	} else if next && orderType == "+" {
		operator = sqlparser.GreaterThanOp
	} else if prev && orderType == "-" {
		operator = sqlparser.GreaterThanOp
		orderDirection = sqlparser.AscOrder
		changeDirection = true
	} else if prev && orderType == "+" {
		operator = sqlparser.LessThanOp
		orderDirection = sqlparser.DescOrder
		changeDirection = true
	}
	return operator, changeDirection, orderDirection
}

// setSorting sets the sorting for the sql query.
func (t *Tabling) setSorting() error {

	// return if sorting is nil
	if t.tabling.Sorting == nil {
		return nil
	}

	// parse sort string to get the sort by and sort type
	sortBy, sortType := t.parseSort(t.tabling.Sorting.Sort)

	// sort by column contains table name
	sortsBy := strings.Split(sortBy, ".")
	if len(sortsBy) == 2 {
		// add order to the statement
		t.stmt.AddOrder(&sqlparser.Order{
			Expr: &sqlparser.ColName{
				Name:      sqlparser.NewIdentifierCI(sortsBy[1]),
				Qualifier: sqlparser.NewTableName(sortsBy[0]),
			},
			Direction: getSortDirection(sortType),
		})

	} else {
		// add order to the statement
		t.stmt.AddOrder(&sqlparser.Order{
			Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(sortBy)},
			Direction: getSortDirection(sortType),
		})
	}

	return nil
}

// addSorting adds the sorting for the sql query.
func (t *Tabling) addSorting(sorting *tablingPkg.Sorting) error {

	// parse sort string to get the sort by and sort type
	sortBy, sortType := t.parseSort(sorting.Sort)

	// sort by column contains table name
	sortsBy := strings.Split(sortBy, ".")
	if len(sortsBy) == 2 {
		// add order to the statement
		t.stmt.AddOrder(&sqlparser.Order{
			Expr: &sqlparser.ColName{
				Name:      sqlparser.NewIdentifierCI(sortsBy[1]),
				Qualifier: sqlparser.NewTableName(sortsBy[0]),
			},
			Direction: getSortDirection(sortType),
		})

	} else {
		// add order to the statement
		t.stmt.AddOrder(&sqlparser.Order{
			Expr: &sqlparser.ColName{
				Name: sqlparser.NewIdentifierCI(sortBy),
			},
			Direction: getSortDirection(sortType),
		})

	}

	return nil

}

// handleDataCursor handles the data for the cursor pagination.
func (t *Tabling) handleDataCursorMap(data *[]map[string]interface{}) (string, string, error) {

	if t.tabling == nil {
		return "", "", nil
	}

	if t.tabling.Paging == nil {
		return "", "", nil
	}

	var (
		totalData = len(*data)

		sort   = t.tabling.Sorting.Sort
		sortBy string
		// sortType string

		limit            = t.tabling.Paging.Limit
		cursor           = t.tabling.Paging.Cursor
		isFirstPage      = t.tabling.Paging.Cursor == ""
		isNext      bool = true

		next string
		prev string
	)

	if t.tabling.Paging.PagingType != tablingPkg.PagingTypeCursor {
		return next, prev, nil
	}

	if totalData == 0 {
		return next, prev, nil
	}

	// parse sort string to get the sort by and sort type
	sortBy, _ = t.parseSort(sort)

	if cursor != "" {
		decodedCursor, err := t.decodeCursor(cursor)
		if err != nil {
			return "", "", err
		}

		if strings.HasPrefix(decodedCursor, "prev") {
			isNext = false
		}
	}

	// reverse the data if it is previous page
	if !isNext {
		reverse(data)
	}

	if totalData > limit {

		if isNext {
			deleteElement(data, totalData-1)
		} else {
			deleteElement(data, 0)
		}

		// update total data after delete
		totalData = len(*data)

		firstID, ok := (*data)[0][t.tabling.Paging.ColumnID].(string)
		if !ok {
			return "", "", nil
		}

		lastID, ok := (*data)[totalData-1][t.tabling.Paging.ColumnID].(string)
		if !ok {
			return "", "", nil
		}

		var (
			firstSort = fmt.Sprintf("%v", (*data)[0][sortBy])
			lastSort  = fmt.Sprintf("%v", (*data)[totalData-1][sortBy])
		)

		next = t.generateCursor("next", lastID, lastSort)
		if !isFirstPage {
			prev = t.generateCursor("prev", firstID, firstSort)
		}
	} else {
		firstID, ok := (*data)[0][t.tabling.Paging.ColumnID].(string)
		if !ok {
			return "", "", nil
		}

		lastID, ok := (*data)[totalData-1][t.tabling.Paging.ColumnID].(string)
		if !ok {
			return "", "", nil
		}

		var (
			firstSort = fmt.Sprintf("%v", (*data)[0][sortBy])
			lastSort  = fmt.Sprintf("%v", (*data)[totalData-1][sortBy])
		)

		if isNext && !isFirstPage {
			prev = t.generateCursor("prev", firstID, firstSort)
		} else if !isNext {
			next = t.generateCursor("next", lastID, lastSort)
		}
	}

	return next, prev, nil
}

func (t *Tabling) handleDataCursorStruct(data interface{}) (string, string, error) {
	if t.tabling == nil || t.tabling.Paging == nil {
		return "", "", nil
	}

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return "", "", fmt.Errorf("data must be a pointer to a slice")
	}
	v = v.Elem()

	totalData := v.Len()
	if totalData == 0 {
		return "", "", nil
	}

	var (
		sort        = t.tabling.Sorting.Sort
		sortBy, _   = t.parseSort(sort)
		limit       = t.tabling.Paging.Limit
		cursor      = t.tabling.Paging.Cursor
		isFirstPage = cursor == ""
		isNext      = true
		next, prev  string
	)

	if t.tabling.Paging.PagingType != tablingPkg.PagingTypeCursor {
		return next, prev, nil
	}

	if cursor != "" {
		decodedCursor, err := t.decodeCursor(cursor)
		if err != nil {
			return "", "", err
		}
		if strings.HasPrefix(decodedCursor, "prev") {
			isNext = false
		}
	}

	if !isNext {
		reverseStructSlice(data)
	}

	// Keep original total data for cursor check
	originalTotalData := totalData

	if totalData > limit {
		if isNext {
			v.Set(v.Slice(0, limit))
		} else {
			v.Set(v.Slice(1, totalData))
		}
		totalData = v.Len()
	}

	firstID, err := getStructFieldByTag(v.Index(0), vars.TagKey, t.tabling.Paging.ColumnID)
	if err != nil {
		return "", "", err
	}
	lastID, err := getStructFieldByTag(v.Index(totalData-1), vars.TagKey, t.tabling.Paging.ColumnID)
	if err != nil {
		return "", "", err
	}

	firstSort, err := getStructFieldByTag(v.Index(0), vars.TagKey, sortBy)
	if err != nil {
		return "", "", err
	}
	lastSort, err := getStructFieldByTag(v.Index(totalData-1), vars.TagKey, sortBy)
	if err != nil {
		return "", "", err
	}

	// Use original totalData to determine if there's more data
	if originalTotalData > limit {
		next = t.generateCursor("next", lastID, lastSort)
		if !isFirstPage {
			prev = t.generateCursor("prev", firstID, firstSort)
		}
	} else {
		if isNext && !isFirstPage {
			prev = t.generateCursor("prev", firstID, firstSort)
		} else if !isNext {
			next = t.generateCursor("next", lastID, lastSort)
		}
	}

	return next, prev, nil
}

// Helper function to reverse a slice of structs
func reverseStructSlice(data interface{}) {
	v := reflect.ValueOf(data).Elem()
	length := v.Len()
	for i := 0; i < length/2; i++ {
		tmp := v.Index(i).Interface()
		v.Index(i).Set(v.Index(length - i - 1))
		v.Index(length - i - 1).Set(reflect.ValueOf(tmp))
	}
}

// Helper function to get a field by tag
func getStructFieldByTag(v reflect.Value, tagName, tagValue string) (string, error) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get(tagName) == tagValue {

			var tempData map[string]interface{}
			if field.Type.Kind() == reflect.Struct {
				raw, err := json.Marshal(v.Interface())
				if err != nil {
					log.Println("error warning, failed to marshal data:", err)
					return fmt.Sprintf("%v", v.Field(i).Interface()), nil
				}

				err = json.Unmarshal(raw, &tempData)
				if err != nil {
					log.Println("error warning, failed to unmarshal data:", err)
					return fmt.Sprintf("%v", v.Field(i).Interface()), nil
				}

				tempValue := tempData[tagValue]
				if tempValue == nil {
					log.Println("error warning, field with tag", tagValue, "not found")
					return fmt.Sprintf("%v", v.Field(i).Interface()), nil
				}

				tempValueStr, isString := tempValue.(string)
				if isString {
					layout := "2006-01-02T15:04:05.000"
					tempTime, err := time.Parse(layout, tempValueStr)
					if err == nil {
						return tempTime.String(), nil
					}

				}
			}

			return fmt.Sprintf("%v", v.Field(i).Interface()), nil
		}
	}
	return "", fmt.Errorf("field with tag %s=%s not found", tagName, tagValue)
}

// generateCursor generates the cursor.
func (t *Tabling) generateCursor(prefix string, id string, sort string) string {
	return t.encodeCursor(fmt.Sprintf("%s%s%s%s%s", prefix, t.cursorSeparator, id, t.cursorSeparator, sort))
}

// sanitize sanitizes the data.
func (t *Tabling) sanitize(data *[]map[string]interface{}) error {

	for i := 0; i < len(*data); i++ {
		for key := range (*data)[i] {
			val, err := url.Parse(key)
			if err != nil {
				return errors.Wrap(err, "failed to parse query")
			}

			if val.Query().Get("nullable") == "true" {

				(*data)[i][val.Path] = (*data)[i][key]
				delete((*data)[i], key)
			}
		}
	}
	return nil

}

// handleDataOffsetMap handles the data for the offset pagination.
func (t *Tabling) handleDataOffsetMap(data *[]map[string]interface{}) (totalData int, err error) {

	if t.tabling == nil {
		return
	}

	if t.tabling.Paging == nil {
		return
	}

	if t.tabling.Paging.PagingType != tablingPkg.PagingTypeOffset {
		return
	}

	if len(*data) == 0 {
		return
	}

	// ok to avoid panic
	tf, ok := (*data)[0][totalDataColumn].(float64)
	if ok {
		totalData = int(tf)
	}

	if !ok {
		// _ to avoid panic
		ti, _ := (*data)[0][totalDataColumn].(int64)
		totalData = int(ti)
	}

	for i := 0; i < len(*data); i++ {
		delete((*data)[i], totalDataColumn)
	}

	return
}

// handleDataOffsetStruct handles the data for the offset pagination.
func (t *Tabling) handleDataOffsetStruct(data interface{}) (totalData int, err error) {
	if t.tabling == nil || t.tabling.Paging == nil || t.tabling.Paging.PagingType != tablingPkg.PagingTypeOffset {
		return
	}

	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return 0, fmt.Errorf("data must be a pointer to a slice")
	}
	v = v.Elem()

	if v.Len() == 0 {
		return
	}

	// Extract totalData from the first struct's tagged field
	firstStruct := v.Index(0)
	totalField, err := getStructFieldByTagAsFloat64(firstStruct, vars.TagKey, totalDataColumn)
	if err == nil {
		totalData = int(totalField)
	}

	return totalData, nil
}

// Extract field value as float64 for flexibility in handling different numeric types
func getStructFieldByTagAsFloat64(v reflect.Value, tagName, tagValue string) (float64, error) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get(tagName) == tagValue {
			value := v.Field(i).Interface()
			switch v := value.(type) {
			case float64:
				return v, nil
			case int, int32, int64:
				return float64(reflect.ValueOf(value).Int()), nil
			default:
				return 0, fmt.Errorf("field %s is not a numeric type", tagValue)
			}
		}
	}
	return 0, fmt.Errorf("field with tag %s=%s not found", tagName, tagValue)
}

// parseSort parses the sort string.
func (t *Tabling) parseSort(sort string) (sortBy string, sortType string) {

	if strings.HasPrefix(sort, "-") {
		sortBy = strings.TrimPrefix(sort, "-")
		sortType = "-"
	} else {
		sortBy = strings.TrimPrefix(sort, "+")
		sortType = "+"
	}

	return
}

// getSortDirection gets the sort direction.
func getSortDirection(sortType string) sqlparser.OrderDirection {
	switch sortType {
	case "-":
		return sqlparser.DescOrder
	case "+":
		return sqlparser.AscOrder
	default:
		return -1
	}
}

// replaceBindVariables replaces all bind variables with format
// :v1, :v2, etc to ?
func replaceBindVariables(sql string) string {
	// replace all bind variables with format :v1, :v2, etc to ?
	re := regexp.MustCompile(`:\bv\d+\b`)

	// Ganti semua yang match dengan ?
	return re.ReplaceAllString(sql, "?")

}

// Function to reverse a slice of maps
func reverse(data *[]map[string]interface{}) {
	for i, j := 0, len(*data)-1; i < j; i, j = i+1, j-1 {
		(*data)[i], (*data)[j] = (*data)[j], (*data)[i]
	}
}

// Function to delete an element at index from a slice
func deleteElement(s *[]map[string]interface{}, index int) {
	if index < 0 || index >= len(*s) {
		panic("index out of range")
	}
	*s = append((*s)[:index], (*s)[index+1:]...)
}

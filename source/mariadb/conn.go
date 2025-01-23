package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"depoq/log"
	"depoq/vars"

	"depoq/parser"
	"depoq/result"
	"depoq/tabling"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/pkg/errors"
	"github.com/redhajuanda/sqlparser"
)

// DBI is an interface that represents a database interface.
// It is implemented by sqlx.DB and sqlx.Tx.
type DBI interface {
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type MariaDB struct {
	conn   *sqlx.DB
	parser parser.Parser
	log    log.Logger
}

const Source = "mariadb"

type contextKey string

var (
	contextKeyTx = contextKey("tx") // contextKeyTx is a context key used to store the transaction in the context.
)

type ConnectParams struct {
	Host            string
	Port            int
	Username        string
	Password        string
	DatabaseName    string
	Parameters      url.Values
	ConnMaxIdleTime int
	ConnMaxLifetime int
	MaxIdleConns    int
	MaxOpenConns    int
}

// Connect establishes a connection to a MariaDB database using the provided DSN (Data Source Name),
// username, password, and database name. It returns a pointer to a MariaDB struct and an error if any.
// The DSN have the format "mariadb://username:password@protocol(address)/dbname?param=value".
// If the DSN is invalid, an error is returned.
func Connect(params ConnectParams, log log.Logger) (*MariaDB, error) {

	var (
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", params.Username, params.Password, params.Host, params.Port, params.DatabaseName, params.Parameters.Encode())
	)

	// connect to the database
	conn, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// set mapper to use standard tag key
	conn.Mapper = reflectx.NewMapperFunc(vars.TagKey, strings.ToLower)

	// set connection parameters
	conn.SetConnMaxIdleTime(time.Duration(params.ConnMaxIdleTime) * time.Second)
	conn.SetConnMaxLifetime(time.Duration(params.ConnMaxLifetime) * time.Second)
	conn.SetMaxIdleConns(params.MaxIdleConns)
	conn.SetMaxOpenConns(params.MaxOpenConns)

	return &MariaDB{
		conn:   conn,
		parser: parser.New(),
		log:    log,
	}, nil

}

// Ping pings the MariaDB server to check if it is still alive and responsive.
// It returns an error if the ping fails.
func (m *MariaDB) Ping(ctx context.Context) error {

	if err := m.conn.Ping(); err != nil {
		return errors.Wrap(err, "failed to ping MariaDB")
	}

	return nil

}

// Close closes the MariaDB connection.
// It returns an error if there was a problem closing the connection.
func (m *MariaDB) Close(ctx context.Context) error {

	if err := m.conn.Close(); err != nil {
		return errors.Wrap(err, "failed to close connection")
	}

	return nil

}

// Exec executes the given SQL query with optional parameters and returns the result as a slice of maps.
// Each map represents a row in the result set, with column names as keys and corresponding values.
// The query can contain placeholders for parameters, which are replaced by the values provided.
// The parameters are optional and can be of any type.
// The returned error indicates any issues encountered during the execution of the query.
func (m *MariaDB) Exec(ctx context.Context, runner string, data map[string]any, tabling *tabling.Tabling) (*result.Result, error) {

	return m.exec(ctx, m.conn, runner, data, tabling)

}

// ExecTx executes a database query within a transaction.
// It takes a context, a runner configuration, and a map of data as input.
// It returns the query result and an error, if any.
func (m *MariaDB) ExecTx(ctx context.Context, runner string, data map[string]any, tabling *tabling.Tabling) (*result.Result, error) {

	// get tx from context
	tx, err := m.getTx(ctx)
	if err != nil {
		return nil, err
	}

	return m.exec(ctx, tx, runner, data, tabling)

}

// exec executes the given SQL query with optional parameters and returns the result as a slice of maps.
// Each map represents a row in the result set, with column names as keys and corresponding values.
// The query can contain placeholders for parameters, which are replaced by the values provided.
// The parameters are optional and can be of any type.
func (m *MariaDB) exec(ctx context.Context, db DBI, query string, data map[string]any, tablingData *tabling.Tabling) (*result.Result, error) {

	var (
		tabling  *Tabling
		metadata = new(result.Metadata)
		rows     *sqlx.Rows
		res      sql.Result
	)

	// ctx, span := otel.Start(ctx)
	// defer span.End()

	m.log.With(ctx).WithParams(log.Params{"query_og": query, "data": data}).Debug("executing query")

	// parse query
	query, parameters, err := m.parser.Parse(ctx, query, data)
	if err != nil {
		return nil, err
	}

	m.log.With(ctx).WithParams(log.Params{"query_parsed": query, "parameters": parameters}).Debug("query parsed")

	if tablingData != nil {
		tabling = NewTabling(query, tablingData)

		query, err = tabling.Init()
		if err != nil {
			return nil, err
		}

		m.log.With(ctx).WithParams(log.Params{"query_tabling": query}).Debug("query tabling")
	}

	// stmt, err := m.parseQuery(ctx, query)
	// if err != nil {
	// 	return nil, err
	// }

	// // handle the statement based on the type
	// switch stmt.(type) {
	// case *sqlparser.Update, *sqlparser.Insert, *sqlparser.Delete:

	// 	// execute query
	// 	res, err = db.ExecContext(ctx, query, parameters...)
	// 	if err != nil {
	// 		return nil, errors.Wrap(err, "failed to exec query")
	// 	}

	// default:

	// 	// query the database
	// 	rows, err = db.QueryxContext(ctx, query, parameters...)
	// 	if err != nil {
	// 		return nil, errors.Wrap(err, "failed to query query")
	// 	}
	// }

	queryType := m.getQueryType(query)
	m.log.With(ctx).WithParams(log.Params{"query_type": queryType}).Debug("query type")
	switch queryType {
	case "INSERT", "UPDATE", "DELETE":
		// execute query
		res, err = db.ExecContext(ctx, query, parameters...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to exec query")
		}
	case "SELECT":
		// query the database
		rows, err = db.QueryxContext(ctx, query, parameters...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query query")
		}
	default:
		return nil, errors.New("unknown query type")

	}

	responser := &responser{
		rows:        rows,
		res:         res,
		mapScanFunc: MapScan,
		jsonMarshalFunc: func(v interface{}) ([]byte, error) {
			return json.Marshal(v)
		},
		tabling: tabling,
		meta:    metadata,
	} // create and return response

	if rows != nil {
		columns, _ := rows.Columns()
		metadata.Columns = columns
	}
	return result.Init(responser, metadata), nil
}

// getQueryType identifies the type of SQL query
func (m *MariaDB) getQueryType(sql string) string {

	// Trim leading and trailing spaces and convert to uppercase
	sql = strings.TrimSpace(sql)
	sql = strings.ToUpper(sql)

	// check if the query has returning
	if strings.Contains(sql, "RETURNING") {
		return "SELECT"
	}

	if strings.HasPrefix(sql, "WITH") {

		// Match the `WITH` clause including the CTE body and the closing parenthesis
		re := regexp.MustCompile(`(?i)WITH\s+[\s\S]*?\)`)
		// Remove everything that matches the CTE pattern
		remaining := re.ReplaceAllString(sql, "")

		return m.getQueryType(remaining) // Recurse to analyze the main query
	}

	switch {
	case strings.HasPrefix(sql, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(sql, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(sql, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(sql, "SELECT"):
		return "SELECT"
	default:
		return "UNKNOWN"
	}
}

// parseQuery parses the given SQL query and returns the corresponding sqlparser statement.
func (m *MariaDB) parseQuery(_ context.Context, query string) (sqlparser.Statement, error) {

	// create parser
	ps, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create parser")
	}

	// parse sql query to sqlparser statement
	stmt, err := ps.Parse(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse sql")
	}

	return stmt, nil
}

// getDBI returns the database interface (DBI) to be used for executing queries.
// If a transaction is available in the context, it will be used.
// Otherwise, the connection will be used.
func (m *MariaDB) getTx(ctx context.Context) (DBI, error) {

	if tx, ok := ctx.Value(contextKeyTx).(*sqlx.Tx); ok {
		return tx, nil
	}
	return nil, errors.New("failed to get transaction from context")

}

// Begin starts a new transaction in the PostgreSQL database.
// It takes a context.Context as input and returns a new context.Context and an error.
// The returned context.Context contains the transaction information that can be used in subsequent database operations.
// If an error occurs while starting the transaction, it returns nil and the error.
func (m *MariaDB) Begin(ctx context.Context) (context.Context, error) {

	tx, err := m.conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	}

	// create and return a new context with the transaction information
	ctx = context.WithValue(ctx, contextKeyTx, tx)
	return ctx, nil
}

// Commit commits the current transaction.
// It retrieves the transaction from the context and calls the Commit method on it.
// If the transaction is not found in the context, it returns an error.
func (m *MariaDB) Commit(ctx context.Context) error {

	tx, ok := ctx.Value(contextKeyTx).(*sqlx.Tx)
	if !ok {
		return errors.New("failed to commit, transaction not found in context")
	}

	err := tx.Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil

}

// Rollback rolls back the transaction associated with the given context.
// It returns an error if the transaction is not found in the context.
// The rollback operation is performed using the pgx.Tx.Rollback method.
func (m *MariaDB) Rollback(ctx context.Context) error {

	tx, ok := ctx.Value(contextKeyTx).(*sqlx.Tx)
	if !ok {
		return errors.New("failed to rollback, transaction not found in context")
	}

	err := tx.Rollback()
	if err != nil {
		return errors.Wrap(err, "failed to rollback transaction")
	}

	return nil
}

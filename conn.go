package depoq

import (
	"context"
	"sync"

	"depoq/log"

	"depoq/result"
	"depoq/source/mariadb"
	"depoq/tabling"
)

// Executor is an interface that defines the contract for executing operations.
//
//go:generate mockgen --source=conn.go --destination=conn_mock.go --package sikatalog
type Executor interface {

	// Exec executes the operation with the given context, runner configuration, and data.
	// It returns the response and an error if any.
	Exec(ctx context.Context, runner string, data map[string]any, tabling *tabling.Tabling) (*result.Result, error)

	// Ping checks the connectivity of the connection.
	Ping(ctx context.Context) error
}

// Transactioner is an interface that defines the contract for executing operations in a transaction.
// It extends the Executor interface.
type Transactioner interface {

	// Ping checks the connectivity of the connection.
	Ping(ctx context.Context) error

	// Close closes the connection.
	Close(ctx context.Context) error

	// Exec executes the operation with the given context, runner configuration, and data.
	// It returns the response and an error if any.
	Exec(ctx context.Context, runner string, data map[string]any, tabling *tabling.Tabling) (*result.Result, error)

	// ExecTx executes the operation with the given context, runner configuration, and data in a transaction.
	// It returns the response and an error if any.
	ExecTx(ctx context.Context, runner string, data map[string]any, tabling *tabling.Tabling) (*result.Result, error)

	// Begin starts a new transaction and returns a new context.
	Begin(ctx context.Context) (context.Context, error)

	// Commit commits the current transaction.
	Commit(ctx context.Context) error

	// Rollback rolls back the current transaction.
	Rollback(ctx context.Context) error
}

// Connectioner is an interface that defines the contract for managing connections to data sources.
// type Connectioner interface {
// 	getTransactionerConnectionDynamic(ctx context.Context, id string, dataSourceHashed string) (Transactioner, error)
// 	getTransactionerConnectionStatic(ctx context.Context, datasourceCode string) (Transactioner, error)
// 	getConnectionDynamic(ctx context.Context, id string, dataSourceHashed string) (Executor, error)
// 	getConnectionStatic(ctx context.Context, datasourceCode string) (Executor, error)
// 	initDataSources(ctx context.Context, clientID string) error
// }

// Conn is a struct that holds the connections to the data sources.
type Conn struct {
	conn  Executor
	mutex sync.Mutex
	log   log.Logger
}

// newConn creates a new Conn instance.
func newConn(log log.Logger) *Conn {
	return &Conn{
		conn:  nil,
		mutex: sync.Mutex{},
		log:   log,
	}
}

// initDataSources
func (c *Conn) initDataSource(ds *DataSource) error {

	c.log.Debug("initializing data source")

	switch ds.Type {
	case "mariadb":
		conn, err := c.openMariaDB(ds.Config)
		if err != nil {
			return err
		}
		c.conn = conn
	}

	return nil
}

// getTransactionerConnectionStatic returns a Transactioner connection based on the provided context and datasourceCode.
// func (c *Conn) getTransactionerConnectionStatic(ctx context.Context, datasourceCode string) (Transactioner, error) {

// 	conn, err := c.getConnectionStatic(ctx, datasourceCode)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return conn.(Transactioner), err
// }

func (c *Conn) getConnection() Executor {
	return c.conn
}

func (c *Conn) getTransactionerConnection() Transactioner {
	return c.conn.(Transactioner)
}

// openConnection opens a new connection to the specified data source based on the given context and data source.
// func (c *Conn) openConnection(ctx context.Context, dataSource domain.DataSource) (Executor, error) {

// 	var (
// 		executor Executor
// 	)

// 	ctx, span := otel.Start(ctx)
// 	defer span.End()

// 	c.log.With(ctx).WithParam("id", dataSource.ID).Debug("creating new connection")

// 	structs.DefaultTagName = "json"
// 	dataSourceMap := structs.New(dataSource).Map()

// 	sourceType := dataSourceMap["source"].(string)
// 	config := dataSourceMap["config"].(map[string]any)

// 	switch sourceType {

// 	case mariadb.Source, mysql.Source, postgresql.Source, mongodb.Source, snowflake.Source: // open transactioner connection

// 		transactioner, err := c.openTransactionerConnection(sourceType, config[sourceType].(map[string]interface{}))
// 		if err != nil {
// 			return nil, err
// 		}
// 		executor = transactioner

// 	case http.Source: // open http connection

// 		baseURL := config[http.Source].(map[string]any)["base_url"].(string)
// 		defaultTimeout := config[http.Source].(map[string]any)["default_timeout"].(int)

// 		http, err := http.New(baseURL, defaultTimeout, c.log)
// 		if err != nil {
// 			return nil, err
// 		}
// 		executor = http

// 	default:
// 		return nil, errors.New("source type not yet implemented")
// 	}

// 	return executor, nil

// }

// openMariaDB opens a new connection to a MariaDB database.
func (c *Conn) openMariaDB(cfg ConfigDetails) (Transactioner, error) {

	// connect to mariadb
	return mariadb.Connect(
		mariadb.ConnectParams{
			Host:            cfg.Host,
			Port:            cfg.Port,
			Username:        cfg.Username,
			Password:        cfg.Password,
			DatabaseName:    cfg.DatabaseName,
			Parameters:      cfg.Parameters,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
			MaxIdleConns:    cfg.MaxIdleConns,
			MaxOpenConns:    cfg.MaxOpenConns,
		},
		c.log,
	)

}

package depoq

import (
	"context"
	"depoq/log"
	"depoq/trace"
	"io/ioutil"
	stdLog "log"
	"os"
	"path/filepath"
	"strings"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type depoqqer interface {
	Run(runner string)
}

type Client struct {
	conn    *Conn
	log     log.Logger
	runners map[string]string
}

// Init initializes the depoq client with the given datasource name.
func Init(datasourceName string) *Client {

	return initClient(datasourceName)

}

// func InitDB

func initClient(datasourceName string) *Client {

	// load config
	cfg, err := loadConfig("depoq.yaml")
	if err != nil {
		stdLog.Fatalf("error loading yaml config: %v", err)
	}

	// find datasource by name
	ds, err := cfg.FindByName(datasourceName)
	if err != nil {
		stdLog.Fatalf("error finding datasource: %v", err)
	}

	// init log

	logger := log.New("depoq")
	log.SetLevel(logrus.DebugLevel)

	// init conn
	conn := newConn(logger)

	// init data source
	err = conn.initDataSource(ds)
	if err != nil {
		stdLog.Fatalf("error initializing data source: %v", err)
	}

	// init runners
	runners, err := initRunners(cfg.Runner.Paths)
	if err != nil {
		stdLog.Fatalf("error initializing runners: %v", err)
	}

	return &Client{
		conn:    conn,
		log:     logger,
		runners: runners,
	}
}

// initRunners walks through directories, reads all files, and stores their content in a map.
// The map's key is the file name without the extension, and the value is the file's content.
func initRunners(paths []string) (map[string]string, error) {
	contentMap := make(map[string]string)

	for _, rootPath := range paths {
		// Walk through each directory
		err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// Skip directories
			if info.IsDir() {
				return nil
			}

			// Get the file name without the extension
			fileName := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))

			// Read the file content
			content, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			// Store the file name and content in the map
			contentMap[fileName] = string(content)
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return contentMap, nil
}

func (c *Client) Run(runner string) Runnerer {

	return newRunner(runnerParams{
		runnerCode:    runner,
		client:        c,
		log:           c.log,
		inTransaction: false,
	})
}

// WithTransaction initializes a new query with transaction.
// it takes a context, transaction code, and callback as input.
// context is the context of the transaction.
// transaction code is the code of the transaction that will be executed.
// callback is a function that will be executed in the transaction.
// callback takes a context and tx as input.
// tx is a struct that contains the transaction configs.
func (c *Client) WithTransaction(ctx context.Context, callback TxFunc) (out any, err error) {

	// inject request id to context
	ctx = c.injectRequestID(ctx)

	// // get transaction data from sikatalog api
	// c.log.With(ctx).WithParam("tx_code", txCode).Debug("getting transaction data from sikatalog api")
	// tx, err := c.sikatalog.GetTransactionByCode(ctx, txCode)
	// if err != nil {
	// 	return nil, err
	// }

	// get transactioner connection
	c.log.With(ctx).Debug("getting connection")
	conn := c.conn.getTransactionerConnection()
	// conn, err := c.getConnection(ctx, datasourceCode, "")
	// if err != nil {
	// 	return nil, err
	// }

	// begin transaction
	c.log.With(ctx).Debug("beginning transaction")
	ctx, err = conn.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	}

	// defer rollback or commit transaction
	// if panic occurs, rollback transaction
	// if error occurs, rollback transaction
	// if no panic or error occurs, commit transaction
	defer c.handleTransaction(ctx, conn, &err)

	// execute callback
	c.log.With(ctx).Debug("executing callback")
	out, err = callback(ctx, newTx(c, c.log))

	return
}

// handleTransaction handles the transaction logic for a given context and connection.
// It rolls back the transaction if a panic occurs or if an error is passed as input.
// If no panic or error occurs, it commits the transaction.
// It returns an error if there is a failure in rolling back or committing the transaction.
func (c *Client) handleTransaction(ctx context.Context, conn Transactioner, errIn *error) (errOut error) {

	if p := recover(); p != nil {

		c.log.With(ctx).Debug("panic occurred, rolling back transaction")

		err := conn.Rollback(ctx)
		if err != nil {
			errOut = errors.Wrap(err, "failed to rollback transaction")
		}
		panic(p) // re-throw panic after Rollback

	} else if *errIn != nil {

		c.log.With(ctx).Debug("error occurred, rolling back transaction")

		err := conn.Rollback(ctx)
		if err != nil {
			errOut = errors.Wrap(err, "failed to rollback transaction")
		}

	} else {

		c.log.With(ctx).Debug("committing transaction")

		err := conn.Commit(ctx)
		if err != nil {
			errOut = errors.Wrap(err, "failed to commit transaction")
		}

	}
	return
}

// injectRequestID injects request id to context.
func (c *Client) injectRequestID(ctx context.Context) context.Context {

	// if request id is not empty, inject request id to context
	existingRequestID := trace.GetRequestIDFromContext(ctx)

	if existingRequestID == "" { // if existing request id in context is empty, inject new request id to context

		ctx = trace.InjectRequestID(ctx, ulid.Make().String())

	}

	return ctx

}

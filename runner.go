package depoq

import (
	// 	"context"
	"context"
	"depoq/log"
	"depoq/mapper"
	"depoq/tabling"
	"depoq/trace"
	"fmt"
	"io"

	"depoq/result"

	ulid "github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	// "gitlab.sicepat.tech/platform/sikatalog-sdk-go/result"
	// // "gitlab.sicepat.tech/platform/sikatalog-sdk-go.git/log"
	// // "gitlab.sicepat.tech/platform/sikatalog-sdk-go.git/result"
)

// // Runnerer represents the main interface for the Runner.
// //
// //go:generate mockgen --source=runner.go -destination=runner_mock.go --package sikatalog
type Runnerer interface {
	// WithParams initializes a new query with params.
	// Params can be a map or a struct, doesn't matter if you pass its pointer or its value.
	WithParams(interface{}) Runnerer
	// WithParam initializes a new query with param.
	// Param is a key-value pair.
	// The key is the parameter name, and the value is the parameter value.
	// If the parameter already exists, it will be overwritten.
	WithParam(key string, value interface{}) Runnerer
	// // WithCache(key string, ttl time.Duration, shouldCache ...ShouldCache) Runnerer
	WithPaging(Paging) Runnerer
	WithSorting(order string) Runnerer
	ScanMap(dest map[string]interface{}) Runnerer
	ScanMaps(dest *[]map[string]interface{}) Runnerer
	ScanStruct(dest interface{}) Runnerer
	ScanStructs(dest interface{}) Runnerer
	ScanWriter(dest io.Writer) Runnerer
	Execute(ctx context.Context) (*result.Metadata, error)
}

// // Runner is a struct that contains runner configs to be executed.
type Runner struct {
	runnerCode    string
	params        map[string]interface{}
	client        *Client
	log           log.Logger
	inTransaction bool
	requestID     string
	// // cacher        *Cacher
	scanner *Scanner
	tabling *tabling.Tabling
	result  *result.Result
	err     error
}

type runnerParams struct {
	runnerCode    string
	client        *Client
	log           log.Logger
	inTransaction bool
}

// newRunner returns a new Runner.
func newRunner(runnerParams runnerParams) *Runner {

	return &Runner{
		runnerCode:    runnerParams.runnerCode,
		client:        runnerParams.client,
		params:        make(map[string]interface{}),
		log:           runnerParams.log,
		inTransaction: runnerParams.inTransaction,
		// cacher:        &Cacher{},
		result: &result.Result{
			Metadata: &result.Metadata{},
		},
	}

}

// WithParam initializes a new query with param.
// Param is a key-value pair.
// The key is the parameter name, and the value is the parameter value.
// If the parameter already exists, it will be overwritten.
func (r *Runner) WithParam(key string, value interface{}) Runnerer {

	r.params[key] = value
	return r

}

// WithParams initializes a new query with params.
// Params can be a map or a struct, doesn't matter if you pass its pointer or its value.
func (r *Runner) WithParams(params interface{}) Runnerer {

	// check if params is a map
	if p, ok := params.(map[string]interface{}); ok {
		r.params = p
		return r
	}

	// check if params is a pointer to a map
	if p, ok := params.(*map[string]interface{}); ok {
		r.params = *p
		return r
	}

	// check if params is a struct
	if isStruct(params) {

		err := mapper.Decode(params, &r.params)
		if err != nil {
			r.err = errors.Wrap(err, "failed to decode params")
		}

		return r

	}

	r.err = errors.New("params must be a map or a struct")
	return r

}

// WithPaging initializes a new runner with pagination.
// Pagination is a struct that contains pagination parameters.
func (r *Runner) WithPaging(pagination Paging) Runnerer {

	if r.tabling == nil {
		r.tabling = &tabling.Tabling{}
	}

	r.tabling.Paging = &pagination.Paging
	return r

}

// WithSorting initializes a new runner with order.
// Order is a string that contains order parameters.
func (r *Runner) WithSorting(sort string) Runnerer {

	if r.tabling == nil {
		r.tabling = &tabling.Tabling{}
	}

	r.tabling.Sorting = &tabling.Sorting{
		Sort: sort,
	}
	return r

}

// ScanMap initializes a runner with scanner map.
// dest is the destination of the scanner.
// It must be a map.
func (r *Runner) ScanMap(dest map[string]interface{}) Runnerer {

	r.scanner = newScanner(scannerMap, dest)
	return r

}

// ScanMaps initializes a runner with scanner maps.
// dest is the destination of the scanner.
// It must be a pointer to a slice of maps.
func (r *Runner) ScanMaps(dest *[]map[string]interface{}) Runnerer {

	r.scanner = newScanner(scannerMaps, dest)
	return r

}

// ScanStruct initializes a runner with scanner struct.
// dest is the destination of the scanner.
// It must be a pointer to a struct.
func (r *Runner) ScanStruct(dest interface{}) Runnerer {

	r.scanner = newScanner(scannerStruct, dest)
	return r

}

// ScanStructs initializes a runner with scanner structs.
// dest is the destination of the scanner.
// It must be a pointer to a slice of structs.
func (r *Runner) ScanStructs(dest interface{}) Runnerer {

	r.scanner = newScanner(scannerStructs, dest)
	return r

}

// ScanWriter initializes a runner with scanner writer.
// dest is the destination of the scanner.
// It must be a writer.
func (r *Runner) ScanWriter(dest io.Writer) Runnerer {

	r.scanner = newScanner(scannerWriter, dest)
	return r

}

// Execute executes the query and returns the response.
// If an error occurs during the execution, it returns an error.
// If the runner is not allowed to be executed in this transaction, it returns an error.
func (r *Runner) Execute(ctx context.Context) (*result.Metadata, error) {

	// ctx, span := otel.Start(ctx, r.runnerCode)
	// defer span.End()

	fmt.Println("==> params: ", r.params)
	// check if there is an error
	if r.err != nil {
		return nil, r.err
	}

	// prepare context
	ctx = r.prepareContext(ctx)

	// execute runner
	meta, err := r.execute(ctx)
	if err != nil {
		r.log.With(ctx).WithStack(err).Error(err)
		return nil, r.mappingError(err)
	}

	return meta, nil

}

// mappingError maps the error to the predefined error.
func (r *Runner) mappingError(err error) error {

	for k, v := range wrappedError {
		for _, e := range v {
			if err == e {
				return k
			}
		}
	}

	return err
}

// prepare prepares the runner to be executed.
func (r *Runner) prepareContext(ctx context.Context) context.Context {

	// inject request id to context
	ctx = r.injectRequestID(ctx)
	return ctx

}

// injectRequestID injects request id to context.
func (r *Runner) injectRequestID(ctx context.Context) context.Context {

	// if request id is not empty, inject request id to context
	existingRequestID := trace.GetRequestIDFromContext(ctx)

	if existingRequestID == "" && r.requestID != "" { // if existing request id in context is empty and request id is not empty, inject request id to context

		ctx = trace.InjectRequestID(ctx, r.requestID)

	} else if existingRequestID == "" && r.requestID == "" { // if existing request id in context is empty and request id is empty, generate new request id and inject to context

		ctx = trace.InjectRequestID(ctx, ulid.Make().String())

	} // otherwise, request id is already in context

	return ctx

}

// execute executes the query and returns the response.
// If an error occurs during the execution, it returns an error.
// If the runner is not allowed to be executed in this transaction, it returns an error.
func (r *Runner) execute(ctx context.Context) (*result.Metadata, error) {

	var (
		// latencyType = "total_execute_runner"
		conn = Executor(nil)
		res  = &result.Result{}
	)

	// try to get object from cache, if exists, return response from cache
	// exists := r.cacher.tryCache(ctx)
	// if exists {
	// 	return r.result.Metadata, nil
	// }

	// get runner by code from sikatalog api
	runner, err := r.getRunner(ctx)
	if err != nil {
		return nil, err
	}

	// validate runner params
	// err = r.validateRunner(ctx, runner.Config, runner.DataSource.Type)
	// if err != nil {
	// 	return nil, err
	// }

	// get db connection
	r.log.With(ctx).WithParams(log.Params{"runner_code": r.runnerCode}).Debug("getting connection")
	conn = r.client.conn.getConnection()

	// execute runner
	if r.inTransaction {

		transactioner, ok := conn.(Transactioner)
		if !ok {
			return nil, errors.New("connection is not a transaction")
		}

		r.log.With(ctx).WithParams(log.Params{"runner_code": r.runnerCode}).Debug("executing runner in transaction")
		// res, err = transactioner.ExecTx(ctx, runner, r.params, r.tabling)
		res, err = transactioner.ExecTx(ctx, runner, r.params, r.tabling)
		if err != nil {
			return nil, err
		}

	} else {

		r.log.With(ctx).WithParams(log.Params{"runner_code": r.runnerCode}).Debug("executing runner")
		// res, err = conn.Exec(ctx, runner.Config, r.params, r.tabling)
		res, err = conn.Exec(ctx, runner, r.params, r.tabling)
		if err != nil {
			return nil, err
		}

	}

	// scan result
	err = r.scan(ctx, res)
	if err != nil {
		return nil, err
	}

	// response = result.Metadata

	r.result = res

	// httpData := result.Metadata.HTTP
	// err = r.cacher.setCache(ctx)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "failed to set cache")
	// }

	return r.result.Metadata, err

}

// getRunner gets the runner by code.
func (r *Runner) getRunner(_ context.Context) (string, error) {

	if q, ok := r.client.runners[r.runnerCode]; ok {
		return q, nil
	}

	return "", errors.New("runner not found")

}

// scan scans the result to the destination.
func (r *Runner) scan(ctx context.Context, result *result.Result) error {

	if r.scanner == nil {
		r.scanner = newScanner(noScanner, nil)
	}

	// logging debug
	r.log.With(ctx).WithParams(log.Params{"runner_code": r.runnerCode}).Debug("scanning result")

	// scan result
	switch r.scanner.scannerType {
	case scannerMap:

		err := result.Scanner.ScanMap(r.scanner.dest.(map[string]interface{}))
		if err != nil {
			return err
		}

	case scannerMaps:

		err := result.Scanner.ScanMaps(r.scanner.dest.(*[]map[string]interface{}))
		if err != nil {
			return err
		}

	case scannerStruct:

		err := result.Scanner.ScanStruct(r.scanner.dest)
		if err != nil {
			return err
		}

	case scannerStructs:

		err := result.Scanner.ScanStructs(r.scanner.dest)
		if err != nil {
			return err
		}

	case scannerWriter:

		err := result.Scanner.ScanWriter(r.scanner.dest.(io.Writer))
		if err != nil {
			return err
		}

	default:

		r.log.With(ctx).WithParams(log.Params{"runner_code": r.runnerCode}).Debug("no scanner found, closing scanner")
		err := result.Scanner.Close()
		if err != nil {
			return err
		}

	}

	return nil

}

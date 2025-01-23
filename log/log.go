package log

import (
	"context"
	"io"

	"depoq/trace"

	"github.com/sirupsen/logrus"
)

// Logger represent common interface for logging function
type Logger interface {
	With(ctx context.Context) Logger
	WithStack(err error) Logger
	WithParam(key string, value interface{}) Logger
	WithParams(params Params) Logger
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
	Fatalf(format string, args ...interface{})
	Fatal(args ...interface{})
	Infof(format string, args ...interface{})
	Info(args ...interface{})
	Warnf(format string, args ...interface{})
	Warn(args ...interface{})
	Debugf(format string, args ...interface{})
	Debug(args ...interface{})
}

// Params type, used to pass to `WithParams`.
type Params map[string]interface{}

type logger struct {
	*logrus.Entry
}

var logStore *logger

// New returns a new wrapper log
func New(serviceName string) Logger {
	logStore = &logger{logrus.New().WithFields(logrus.Fields{"service": serviceName})}
	return logStore
}

// SetOutput sets the logger output.
func SetOutput(output io.Writer) {
	logStore.Logger.SetOutput(output)
}

// SetFormatter sets the logger formatter.
func SetFormatter(formatter logrus.Formatter) {
	logStore.Logger.SetFormatter(formatter)
}

// SetLevel sets the logger level.
func SetLevel(level logrus.Level) {
	logStore.Logger.SetLevel(level)
}

// With reads requestId from context and adds to log field
func (l *logger) With(ctx context.Context) Logger {

	var le *logrus.Entry
	if ctx != nil {
		if id, ok := ctx.Value(trace.RequestIDKey).(string); ok {
			le = l.WithField("request_id", id)
		}
	}
	return &logger{le}

}

// WithStack adds stack trace to log field
func (l *logger) WithStack(err error) Logger {
	stack := MarshalStack(err)
	return &logger{l.WithField("stack", stack)}
}

// WithParam adds key value to log field
func (l *logger) WithParam(key string, value interface{}) Logger {

	return &logger{l.WithField(key, value)}
}

// WithParams adds params to log field
func (l *logger) WithParams(params Params) Logger {
	return &logger{l.WithFields(logrus.Fields(params))}
}

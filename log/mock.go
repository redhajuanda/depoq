package log

import "context"

type loggerMock struct {
}

func NewMock() Logger {
	return &loggerMock{}
}

func (l loggerMock) With(ctx context.Context) Logger {
	return loggerMock{}
}

func (l loggerMock) WithStack(err error) Logger {
	return loggerMock{}
}

func (l loggerMock) WithParam(key string, value interface{}) Logger {
	return loggerMock{}
}

func (l loggerMock) WithParams(params Params) Logger {
	return loggerMock{}
}

func (l loggerMock) Errorf(format string, args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Error(args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Fatalf(format string, args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Fatal(args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Infof(format string, args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Info(args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Warnf(format string, args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Warn(args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Debugf(format string, args ...interface{}) {
	// do nothing, it's just a mock
}

func (l loggerMock) Debug(args ...interface{}) {
	// do nothing, it's just a mock
}

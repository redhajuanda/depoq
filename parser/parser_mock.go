// Code generated by MockGen. DO NOT EDIT.
// Source: parser/parser.go
//
// Generated by this command:
//
//	mockgen -source=parser/parser.go -destination=parser/parser_mock.go -package=parser
//

// Package parser is a generated GoMock package.
package parser

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockParser is a mock of Parser interface.
type MockParser struct {
	ctrl     *gomock.Controller
	recorder *MockParserMockRecorder
	isgomock struct{}
}

// MockParserMockRecorder is the mock recorder for MockParser.
type MockParserMockRecorder struct {
	mock *MockParser
}

// NewMockParser creates a new mock instance.
func NewMockParser(ctrl *gomock.Controller) *MockParser {
	mock := &MockParser{ctrl: ctrl}
	mock.recorder = &MockParserMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockParser) EXPECT() *MockParserMockRecorder {
	return m.recorder
}

// Parse mocks base method.
func (m *MockParser) Parse(ctx context.Context, queryTemplate string, data map[string]any) (string, []any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Parse", ctx, queryTemplate, data)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].([]any)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Parse indicates an expected call of Parse.
func (mr *MockParserMockRecorder) Parse(ctx, queryTemplate, data any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Parse", reflect.TypeOf((*MockParser)(nil).Parse), ctx, queryTemplate, data)
}

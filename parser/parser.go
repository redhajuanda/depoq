package parser

import (
	"context"
	"html/template"

	"github.com/VauntDev/tqla"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

//go:generate mockgen --source=parser.go --destination=parser_mock.go --package=parser
type Parser interface {
	Parse(ctx context.Context, queryTemplate string, data map[string]any) (string, []interface{}, error)
}

type parser struct{}

func New() *parser {
	return &parser{}
}

// Parse parses the given query by replacing the parameters with placeholders.
// the sanitization is performed using the tqla package.
// tqla is a small light weight text parser that wraps the golang text/template standard library.
// the primary purpose of tqla is to parse a text template and replace any variable with a placeholder.
// variables that are replaced with placeholders are added to an args slice that can be passed to standard db driver.
// tqla prevents sql injection by leveraging DB placeholders as described in the following article:
// https://go.dev/doc/database/sql-injection
func (p *parser) Parse(ctx context.Context, queryTemplate string, data map[string]any) (string, []interface{}, error) {

	const (
		latencyType = "sanitize_query"
	)

	// ctx, span := otel.Start(ctx)
	// defer span.End()

	// create a new parser with the default placeholder set to ?
	parser, err := tqla.New(tqla.WithPlaceHolder(tqla.Question), tqla.WithFuncMap(template.FuncMap{
		"IsTimeZero":    IsTimeZero,
		"IsTimeNotZero": IsTimeNotZero,
		"sub":           func(x int) int { return x - 1 },
	}))
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to parse query")
	}

	// compile query
	query, parameters, err := parser.Compile(queryTemplate, data)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to compile query")
	}

	// interpolate query
	return p.interpolateQuery(ctx, query, parameters...)

}

// interpolateQuery interpolates the given query with the provided parameters.
func (p *parser) interpolateQuery(_ context.Context, query string, args ...interface{}) (string, []interface{}, error) {

	// interpolate query
	query, parameters, err := sqlx.In(query, args...)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to interpolate query")
	}

	return query, parameters, nil

}

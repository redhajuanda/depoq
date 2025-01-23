package trace

import (
	"context"
	"net/http"

	"github.com/oklog/ulid/v2"
)

// HeaderRequestID is the name of the HTTP Header which contains the request id.
// Exported so that it can be changed by developers
var (
	HeaderRequestID     = "X-Request-ID"
	HeaderAuthorization = "Authorization"
)

type (
	contextKey int
)

const (
	RequestIDKey contextKey = iota
)

// InjectRequestID returns a context which knows the request ID
func InjectRequestID(ctx context.Context, requestID string) context.Context {

	ctx = context.WithValue(ctx, RequestIDKey, requestID)
	return ctx
}

// InjectRequestIDFromRequest returns a context which knows the request ID in the given request.
// If the request does not contain the request ID, it will generate a new one.
// The request ID is stored in the context with the key requestIDKey.
// The request ID can be retrieved from the context using the function GetRequestIDFromContext.
func InjectRequestIDFromRequest(req *http.Request) *http.Request {

	id := getRequestID(req)
	if id == "" {
		id = ulid.Make().String()
	}

	// inject request id from the request to the context and return the new context
	ctx := context.WithValue(req.Context(), RequestIDKey, id)

	// set the context back to the request
	req = req.WithContext(ctx)

	return req
}

// GetRequestIDFromContext returns the request ID from the given context.
// If the context does not contain the request ID, it will return an empty string.
func GetRequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return ""
}

// getRequestID extracts the correlation ID from the HTTP request
func getRequestID(req *http.Request) string {
	return req.Header.Get(HeaderRequestID)
}

// GenerateRequestID generates a new request ID using UUID v4
func GenerateRequestID() string {
	return ulid.Make().String()
}

// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/textproto"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const (
	optionHTTPHeaderPrefix          = "trino.http_header."
	optionStatementHTTPHeaderPrefix = "trino.statement.http_header."
)

// reservedHeaders are managed by trino-go-client (which tracks the state of
// most of them across requests) or by net/http, or are hop-by-hop headers,
// and cannot be overridden.
var reservedHeaders = map[string]struct{}{
	"Accept-Encoding":             {},
	"Authorization":               {},
	"Connection":                  {},
	"Content-Length":              {},
	"Content-Type":                {},
	"Host":                        {},
	"Keep-Alive":                  {},
	"Proxy-Connection":            {},
	"Te":                          {}, // codespell:ignore
	"Trailer":                     {},
	"Transfer-Encoding":           {},
	"Upgrade":                     {},
	"X-Trino-Catalog":             {},
	"X-Trino-Client-Capabilities": {},
	"X-Trino-Extra-Credential":    {},
	"X-Trino-Language":            {},
	"X-Trino-Original-Roles":      {},
	"X-Trino-Original-User":       {},
	"X-Trino-Path":                {},
	"X-Trino-Prepared-Statement":  {},
	"X-Trino-Query-Data-Encoding": {},
	"X-Trino-Role":                {},
	"X-Trino-Schema":              {},
	"X-Trino-Session":             {},
	"X-Trino-Time-Zone":           {},
	"X-Trino-Transaction-Id":      {},
	"X-Trino-User":                {},
}

// parseHeaderOption returns the canonical header name for an option key with
// the given prefix, and checks that the header may be set by the user.
func parseHeaderOption(key, prefix, value string) (string, error) {
	name := strings.TrimPrefix(key, prefix)
	if !httpguts.ValidHeaderFieldName(name) {
		return "", fmt.Errorf("invalid HTTP header name %q", name)
	}
	name = textproto.CanonicalMIMEHeaderKey(name)
	if _, ok := reservedHeaders[name]; ok {
		return "", fmt.Errorf("HTTP header %s is managed by the driver and cannot be set", name)
	}
	if !httpguts.ValidHeaderFieldValue(value) {
		return "", fmt.Errorf("invalid value for HTTP header %s", name)
	}
	return name, nil
}

// statementHeadersKey is the context key for the statement-level headers of a
// query.  They are carried on the query context, which trino-go-client uses
// for every request of the query.
type statementHeadersKey struct{}

// withStatementHeaders returns a context carrying a copy of the statement
// headers, so that later changes to the statement options do not affect
// queries that are already running.
func withStatementHeaders(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return context.WithValue(ctx, statementHeadersKey{}, maps.Clone(headers))
}

// headerTransport sets the headers from driver options on requests to the
// Trino server.  Database-level headers override those set by
// trino-go-client, and statement-level headers override database-level ones.
type headerTransport struct {
	base        http.RoundTripper
	scheme      string
	host        string
	sessionUser *string
	headers     http.Header
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	stmtHeaders, hasStmt := req.Context().Value(statementHeadersKey{}).(map[string]string)
	toServer := req.URL.Scheme == t.scheme && req.URL.Host == t.host
	if !toServer || (t.sessionUser == nil && len(t.headers) == 0 && !hasStmt) {
		return t.base.RoundTrip(req)
	}

	req = req.Clone(req.Context())
	if t.sessionUser != nil {
		req.Header.Set("X-Trino-User", *t.sessionUser)
	}
	for name, values := range t.headers {
		req.Header[name] = slices.Clone(values)
	}
	for name, value := range stmtHeaders {
		req.Header.Set(name, value)
	}
	return t.base.RoundTrip(req)
}

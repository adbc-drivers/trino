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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"
)

const (
	optionHTTPHeaderPrefix          = "trino.http_header."
	optionStatementHTTPHeaderPrefix = "trino.statement.http_header."
	optionStatementClientTags       = "trino.statement.client_tags"

	clientTagsHeader = "X-Trino-Client-Tags"
)

// reservedHeaders are managed by trino-go-client (which tracks the state of
// most of them across requests) or by net/http, and cannot be overridden.
var reservedHeaders = map[string]struct{}{
	"Authorization":               {},
	"Content-Length":              {},
	"Content-Type":                {},
	"Host":                        {},
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
	statementHeadersParam:         {},
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

// parseClientTags splits a comma-separated list of client tags.
func parseClientTags(value string) ([]string, error) {
	tags := strings.Split(value, ",")
	for i, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			return nil, errors.New("client tags must not be empty")
		}
		tags[i] = tag
	}
	return tags, nil
}

// mergeClientTags appends tags to the comma-separated header value, skipping
// tags that are already present.
func mergeClientTags(header string, tags []string) string {
	var merged []string
	for tag := range strings.SplitSeq(header, ",") {
		if tag = strings.TrimSpace(tag); tag != "" && !slices.Contains(merged, tag) {
			merged = append(merged, tag)
		}
	}
	for _, tag := range tags {
		if !slices.Contains(merged, tag) {
			merged = append(merged, tag)
		}
	}
	return strings.Join(merged, ",")
}

// statementHeadersParam carries statement-level headers to headerTransport.
// It is passed as a query parameter, which trino-go-client sends as a request
// header, and headerTransport removes it before the request is sent.
// (sqlwrapper does not propagate the query context, so it cannot be used.)
const statementHeadersParam = "X-Trino-Adbc-Statement-Headers"

type statementHeaders struct {
	Headers    map[string]string `json:"headers,omitempty"`
	ClientTags []string          `json:"client_tags,omitempty"`
}

func (h statementHeaders) encode() string {
	// Marshaling strings cannot fail.
	data, _ := json.Marshal(h)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeStatementHeaders(value string) (statementHeaders, error) {
	var h statementHeaders
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return h, err
	}
	err = json.Unmarshal(data, &h)
	return h, err
}

// headerTransport sets the headers from driver options on requests to the
// Trino server.  Database-level headers override those set by
// trino-go-client, statement-level headers override database-level ones,
// and statement client tags are merged into the resulting client tags.
type headerTransport struct {
	base        http.RoundTripper
	scheme      string
	host        string
	sessionUser string
	headers     http.Header
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	encoded := req.Header.Get(statementHeadersParam)
	toServer := req.URL.Scheme == t.scheme && req.URL.Host == t.host
	if encoded == "" && (!toServer || (t.sessionUser == "" && len(t.headers) == 0)) {
		return t.base.RoundTrip(req)
	}

	req = req.Clone(req.Context())
	req.Header.Del(statementHeadersParam)
	if !toServer {
		return t.base.RoundTrip(req)
	}
	if t.sessionUser != "" {
		req.Header.Set("X-Trino-User", t.sessionUser)
	}
	for name, values := range t.headers {
		req.Header[name] = slices.Clone(values)
	}
	if encoded != "" {
		stmt, err := decodeStatementHeaders(encoded)
		if err != nil {
			return nil, fmt.Errorf("trino: invalid statement headers: %w", err)
		}
		for name, value := range stmt.Headers {
			req.Header.Set(name, value)
		}
		if len(stmt.ClientTags) > 0 {
			req.Header.Set(clientTagsHeader, mergeClientTags(req.Header.Get(clientTagsHeader), stmt.ClientTags))
		}
	}
	return t.base.RoundTrip(req)
}

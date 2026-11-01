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
	"database/sql"
	"net/textproto"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/trinodb/trino-go-client/trino"
)

type trinoStatement struct {
	*sqlwrapper.StatementImplBase

	lastQueryId atomic.Pointer[string]

	headers    map[string]string
	clientTags []string
}

func (st *trinoStatement) SetSqlQuery(ctx context.Context, query string) error {
	// Trino's server rejects trailing statement terminators.
	query = strings.TrimRightFunc(query, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})
	return st.StatementImplBase.SetSqlQuery(ctx, query)
}

func (st *trinoStatement) GetOption(ctx context.Context, key string) (string, error) {
	switch {
	case key == "trino.statement.last_query_id":
		val := st.lastQueryId.Load()
		if val == nil {
			return "", nil
		}
		return *val, nil
	case key == optionStatementClientTags:
		return strings.Join(st.clientTags, ","), nil
	case strings.HasPrefix(key, optionStatementHTTPHeaderPrefix):
		name := textproto.CanonicalMIMEHeaderKey(strings.TrimPrefix(key, optionStatementHTTPHeaderPrefix))
		return st.headers[name], nil
	}
	return st.StatementImplBase.GetOption(ctx, key)
}

func (st *trinoStatement) SetOption(ctx context.Context, key, val string) error {
	switch {
	case key == optionStatementClientTags:
		if val == "" {
			st.clientTags = nil
			return nil
		}
		tags, err := parseClientTags(val)
		if err != nil {
			return st.Base().ErrorHelper.InvalidArgument("invalid option %s: %s", key, err)
		}
		st.clientTags = tags
		return nil
	case strings.HasPrefix(key, optionStatementHTTPHeaderPrefix):
		name, err := parseHeaderOption(key, optionStatementHTTPHeaderPrefix, val)
		if err != nil {
			return st.Base().ErrorHelper.InvalidArgument("invalid option %s: %s", key, err)
		}
		if val == "" {
			delete(st.headers, name)
			return nil
		}
		if st.headers == nil {
			st.headers = map[string]string{}
		}
		st.headers[name] = val
		return nil
	}
	return st.StatementImplBase.SetOption(ctx, key, val)
}

func (st *trinoStatement) GetAdditionalExecParams() []any {
	params := []any{
		sql.Named("X-Trino-Progress-Callback", trinoProgressCallback{st}),
		sql.Named("X-Trino-Progress-Callback-Period", 5*time.Second),
	}
	if len(st.headers) > 0 || len(st.clientTags) > 0 {
		headers := statementHeaders{Headers: st.headers, ClientTags: st.clientTags}
		params = append(params, sql.Named(statementHeadersParam, headers.encode()))
	}
	return params
}

func (st *trinoStatement) MakeTypeConverter(vendorName string) sqlwrapper.TypeConverter {
	return typeConverter
}

type trinoProgressCallback struct {
	st *trinoStatement
}

func (cb trinoProgressCallback) Update(info trino.QueryProgressInfo) {
	cb.st.lastQueryId.Store(&info.QueryId)
}

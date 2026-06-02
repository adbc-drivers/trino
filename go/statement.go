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
	"sync/atomic"
	"time"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/trinodb/trino-go-client/trino"
)

type trinoStatement struct {
	*sqlwrapper.StatementImplBase

	lastQueryId atomic.Pointer[string]
}

func (st *trinoStatement) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case "trino.statement.last_query_id":
		val := st.lastQueryId.Load()
		if val == nil {
			return "", nil
		}
		return *val, nil
	}
	return st.StatementImplBase.GetOption(ctx, key)
}

func (st *trinoStatement) GetAdditionalExecParams() []any {
	return []any{
		sql.Named("X-Trino-Progress-Callback", trinoProgressCallback{st}),
		sql.Named("X-Trino-Progress-Callback-Period", 5*time.Second),
	}
}

type trinoProgressCallback struct {
	st *trinoStatement
}

func (cb trinoProgressCallback) Update(info trino.QueryProgressInfo) {
	cb.st.lastQueryId.Store(&info.QueryId)
}

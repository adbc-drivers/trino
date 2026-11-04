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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithStatementHeaders(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, ctx, withStatementHeaders(ctx, nil))

	// The context keeps a copy, so a running query is not affected by later
	// changes to the statement options.
	headers := map[string]string{"X-Gateway": "statement"}
	ctx = withStatementHeaders(ctx, headers)
	headers["X-Gateway"] = "changed"

	stmtHeaders, ok := ctx.Value(statementHeadersKey{}).(map[string]string)
	assert.True(t, ok)
	assert.Equal(t, map[string]string{"X-Gateway": "statement"}, stmtHeaders)
}

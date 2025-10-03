// Copyright (c) 2025 ADBC Drivers Contributors
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
	"fmt"
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
)

// TrinoDBFactory provides Trino-specific database connection creation.
// It handles Trino DSN formatting and connection parameters.
type TrinoDBFactory struct{}

// CreateDB creates a *sql.DB using sql.Open with a Trino-specific DSN.
func (f *TrinoDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error) {
	dsn, err := f.buildTrinoDSN(opts)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// buildTrinoDSN constructs a Trino DSN from the provided options.
func (f *TrinoDBFactory) buildTrinoDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// If no credentials provided, return original URI
	if username == "" && password == "" {
		return baseURI, nil
	}

	// Handle both URI and native Trino DSN formats
	return f.buildTrinoDSNFromURI(baseURI, username, password)
}

// buildTrinoDSNFromURI handles Trino DSN formats.
func (f *TrinoDBFactory) buildTrinoDSNFromURI(baseURI, username, password string) (string, error) {
	// Handle HTTP/HTTPS URLs (native Trino DSN format)
	if strings.HasPrefix(baseURI, "http://") || strings.HasPrefix(baseURI, "https://") {
		return f.buildFromHTTPURI(baseURI, username, password)
	}

	// Handle plain host string - assume HTTPS
	return f.buildFromPlainHost(baseURI, username, password)
}

// buildFromHTTPURI handles http:// and https:// URI formats.
func (f *TrinoDBFactory) buildFromHTTPURI(baseURI, username, password string) (string, error) {
	u, err := url.Parse(baseURI)
	if err != nil {
		return "", fmt.Errorf("invalid Trino URI format: %w", err)
	}

	// Start with the existing URL structure
	var dsnBuilder strings.Builder
	dsnBuilder.WriteString(u.Scheme)
	dsnBuilder.WriteString("://")

	// Add user credentials to the URL authority if provided
	if username != "" {
		dsnBuilder.WriteString(username)
		if password != "" {
			dsnBuilder.WriteString(":")
			dsnBuilder.WriteString(password)
		}
		dsnBuilder.WriteString("@")
	} else if u.User != nil {
		// Use existing user info if no override provided
		if pwd, ok := u.User.Password(); ok {
			dsnBuilder.WriteString(fmt.Sprintf("%s:%s@", u.User.Username(), pwd))
		} else {
			dsnBuilder.WriteString(fmt.Sprintf("%s@", u.User.Username()))
		}
	}

	// Add host and port
	dsnBuilder.WriteString(u.Host)

	// Add path if present
	if u.Path != "" {
		dsnBuilder.WriteString(u.Path)
	}

	// Preserve original query parameters
	if u.RawQuery != "" {
		dsnBuilder.WriteString("?")
		dsnBuilder.WriteString(u.RawQuery)
	}

	return dsnBuilder.String(), nil
}

// buildFromPlainHost handles plain host strings by building HTTPS URLs.
func (f *TrinoDBFactory) buildFromPlainHost(hostString, username, password string) (string, error) {
	// Build HTTPS URL from plain host
	var dsnBuilder strings.Builder
	dsnBuilder.WriteString("https://")

	// Add credentials to URL authority if provided
	if username != "" {
		dsnBuilder.WriteString(username)
		if password != "" {
			dsnBuilder.WriteString(":")
			dsnBuilder.WriteString(password)
		}
		dsnBuilder.WriteString("@")
	}

	// Add host (could include port)
	dsnBuilder.WriteString(hostString)

	return dsnBuilder.String(), nil
}

// Ensure TrinoDBFactory implements sqlwrapper.DBFactory
var _ sqlwrapper.DBFactory = (*TrinoDBFactory)(nil)

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
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/trinodb/trino-go-client/trino"
)

// TrinoDBFactory provides Trino-specific database connection creation.
// It handles Trino DSN formatting and connection parameters.
type TrinoDBFactory struct {
	errorHelper driverbase.ErrorHelper
}

// NewTrinoDBFactory creates a new TrinoDBFactory with proper error handling.
func NewTrinoDBFactory() *TrinoDBFactory {
	return &TrinoDBFactory{
		errorHelper: driverbase.ErrorHelper{DriverName: "trino"},
	}
}

// CreateDB creates a *sql.DB using sql.Open with a Trino-specific DSN.
func (f *TrinoDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error) {
	dsn, err := f.buildTrinoDSN(opts)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// buildTrinoDSN constructs a Trino DSN from the provided options.
// Handles the following scenarios:
//  1. Full DSN, no separate credentials:
//     Example: "https://user:pass@localhost:8080?catalog=default"
//     → Returned as-is.
//  2. Plain host + credentials:
//     Example: baseURI="localhost:8080", username="user", password="secret"
//     → Produces "https://user:secret@localhost:8080".
//  3. Full DSN + override credentials:
//     Example: baseURI="https://old:old@localhost:8080?catalog=default", username="new", password="newpass"
//     → Credentials are replaced.
func (f *TrinoDBFactory) buildTrinoDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		return "", f.errorHelper.InvalidArgument("missing required option %s", adbc.OptionKeyURI)
	}

	// If no credentials provided, return original URI
	if username == "" && password == "" {
		return baseURI, nil
	}

	// Handle both URI and native Trino DSN formats
	return f.buildTrinoDSNFromURI(baseURI, username, password)
}

// buildTrinoDSNFromURI handles Trino DSN formats using trino.Config.
func (f *TrinoDBFactory) buildTrinoDSNFromURI(baseURI, username, password string) (string, error) {
	var cfg *trino.Config
	var err error

	if strings.HasPrefix(baseURI, "http://") || strings.HasPrefix(baseURI, "https://") {
		// Parse existing Trino DSN manually since trino.ParseDSN doesn't exist
		cfg, err = f.parseTrinoURI(baseURI)
		if err != nil {
			return "", f.errorHelper.InvalidArgument("invalid Trino DSN format: %v", err)
		}
	} else {
		// Treat as plain host string - assume HTTPS
		cfg = &trino.Config{
			ServerURI: "https://" + baseURI,
		}
	}

	// Override credentials if provided
	if username != "" || password != "" {
		// Update the ServerURI with new credentials
		if err := f.updateConfigCredentials(cfg, username, password); err != nil {
			return "", err
		}
	}

	return cfg.FormatDSN()
}

// parseTrinoURI manually parses a Trino URI into a Config since trino.ParseDSN doesn't exist.
func (f *TrinoDBFactory) parseTrinoURI(uri string) (*trino.Config, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	cfg := &trino.Config{
		ServerURI: uri,
	}

	// Extract query parameters and map them to Config fields
	if u.RawQuery != "" {
		values := u.Query()
		if catalog := values.Get("catalog"); catalog != "" {
			cfg.Catalog = catalog
		}
		if schema := values.Get("schema"); schema != "" {
			cfg.Schema = schema
		}
		if source := values.Get("source"); source != "" {
			cfg.Source = source
		}
		if customClient := values.Get("custom_client"); customClient != "" {
			cfg.CustomClientName = customClient
		}
		// Parse session_properties if present
		if sessionProps := values.Get("session_properties"); sessionProps != "" {
			cfg.SessionProperties = make(map[string]string)
			for _, prop := range strings.Split(sessionProps, ",") {
				if parts := strings.SplitN(prop, "=", 2); len(parts) == 2 {
					cfg.SessionProperties[parts[0]] = parts[1]
				}
			}
		}
	}

	return cfg, nil
}

// updateConfigCredentials updates the ServerURI in the config with new credentials.
func (f *TrinoDBFactory) updateConfigCredentials(cfg *trino.Config, username, password string) error {
	// Parse the current ServerURI
	if cfg.ServerURI == "" {
		return f.errorHelper.InvalidArgument("config ServerURI is empty")
	}

	u, err := url.Parse(cfg.ServerURI)
	if err != nil {
		return f.errorHelper.InvalidArgument("invalid ServerURI format: %v", err)
	}

	// Set new credentials
	if username != "" {
		if password != "" {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
	}

	cfg.ServerURI = u.String()
	return nil
}

// Ensure TrinoDBFactory implements sqlwrapper.DBFactory
var _ sqlwrapper.DBFactory = (*TrinoDBFactory)(nil)

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
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/trinodb/trino-go-client/trino"
	"github.com/zalando/go-keyring"
)

const (
	// optionExternalAuthentication logs in through the server's external
	// authentication (OAuth 2.0), like the externalAuthentication URI
	// parameter, which it overrides.
	optionExternalAuthentication = "trino.external_authentication"
	// optionTokenCache selects where the token obtained through external
	// authentication is kept.
	optionTokenCache = "trino.token_cache"
	// tokenCacheKeyring also keeps the token in the OS keyring, so that later
	// processes reuse it. This is the default.
	tokenCacheKeyring = "keyring"
	// tokenCacheMemory keeps the token in memory only.
	tokenCacheMemory = "memory"

	keyringService = "adbc-driver-trino"
	// keyringChunkSize keeps each keyring entry under the Windows Credential
	// Manager limit of 2560 bytes and the macOS limit of about 3000 bytes.
	keyringChunkSize    = 1000
	keyringChunksPrefix = "adbc-trino-chunks:"
)

// keyringBackend is the subset of the go-keyring API the token cache uses.
type keyringBackend interface {
	Set(service, user, password string) error
	Get(service, user string) (string, error)
	Delete(service, user string) error
}

type osKeyring struct{}

func (osKeyring) Set(service, user, password string) error {
	return keyring.Set(service, user, password)
}
func (osKeyring) Get(service, user string) (string, error) { return keyring.Get(service, user) }
func (osKeyring) Delete(service, user string) error        { return keyring.Delete(service, user) }

// tokenCache implements trino.TokenCache. The token is kept in memory, since
// the client reads it for every request, and written through to keyring
// unless it is nil.
type tokenCache struct {
	key     string
	keyring keyringBackend
	logger  *slog.Logger

	mu     sync.Mutex
	token  string
	loaded bool
}

var _ trino.TokenCache = (*tokenCache)(nil)

// externalAuthOptions returns whether opts log in through external
// authentication, which is uriValue (the externalAuthentication URI
// parameter) unless the option is set, and where the token is kept.
func externalAuthOptions(opts map[string]string, uriValue bool) (externalAuth bool, tokenCache string, err error) {
	tokenCache = cmp.Or(opts[optionTokenCache], tokenCacheKeyring)
	if tokenCache != tokenCacheKeyring && tokenCache != tokenCacheMemory {
		return false, "", fmt.Errorf("invalid value for %s: %q (expected %q or %q)", optionTokenCache, tokenCache, tokenCacheKeyring, tokenCacheMemory)
	}
	value, ok := opts[optionExternalAuthentication]
	if !ok {
		return uriValue, tokenCache, nil
	}
	externalAuth, err = strconv.ParseBool(value)
	if err != nil {
		return false, "", fmt.Errorf("invalid value for %s: %q", optionExternalAuthentication, value)
	}
	return externalAuth, tokenCache, nil
}

// externalAuthConnector returns the Connector for cfg, parsed from dsn, which
// keeps its token as tokenCacheMode says. Databases with the same
// configuration (e.g. the threads of dbt) share the Connector, so that while
// one logs in, the others wait for its token rather than also opening the
// browser. The custom client, which holds the options that are not in dsn
// (headers, timeouts and TLS), completes the configuration.
func (f *TrinoDBFactory) externalAuthConnector(cfg *trino.Config, dsn, tokenCacheMode string, logger *slog.Logger) (*trino.Connector, error) {
	key := tokenCacheMode + "|" + dsn + "|" + cfg.CustomClientName
	if connector, ok := f.connectors.Load(key); ok {
		return connector.(*trino.Connector), nil
	}
	var err error
	cfg.TokenCache, err = f.tokenCache(cfg.ServerURI, tokenCacheMode, logger)
	if err != nil {
		return nil, err
	}
	cfg.RedirectHandler = f.redirect
	connector, err := trino.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	shared, _ := f.connectors.LoadOrStore(key, connector)
	return shared.(*trino.Connector), nil
}

// tokenCache returns the cache for the server and user of serverURI, which
// all the Connectors for them share: when one logs in again, the others then
// reuse its token instead of logging in too.
func (f *TrinoDBFactory) tokenCache(serverURI, mode string, logger *slog.Logger) (*tokenCache, error) {
	var backend keyringBackend
	if mode == tokenCacheKeyring {
		backend = f.keyring
		if backend == nil {
			backend = osKeyring{}
		}
	}
	cache, err := newTokenCache(serverURI, backend, logger)
	if err != nil {
		return nil, err
	}
	// The memory cache is kept apart, so that it never writes to the keyring.
	shared, _ := f.tokenCaches.LoadOrStore(mode+"|"+cache.key, cache)
	return shared.(*tokenCache), nil
}

// newTokenCache returns a cache for the token of the server and user of
// serverURI, which also keeps it in backend unless that is nil.
func newTokenCache(serverURI string, backend keyringBackend, logger *slog.Logger) (*tokenCache, error) {
	key, err := tokenCacheKey(serverURI)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &tokenCache{key: key, keyring: backend, logger: logger}, nil
}

// tokenCacheKey identifies the server and user a token belongs to.
func tokenCacheKey(serverURI string) (string, error) {
	u, err := driverbase.SafeParseURL(serverURI)
	if err != nil {
		return "", fmt.Errorf("could not parse Trino server URI: %w", err)
	}
	key := strings.ToLower(u.Scheme) + "://" + serverHost(u)
	if user := u.User.Username(); user != "" {
		key = user + "@" + key
	}
	return key, nil
}

func (c *tokenCache) Token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		c.loaded = true
		if c.keyring != nil {
			token, err := c.load()
			if err != nil && !errors.Is(err, keyring.ErrNotFound) {
				c.logger.Warn("could not read the Trino token from the keyring", "error", err)
			}
			c.token = token
		}
	}
	return c.token
}

// SetToken is called with "" when the server rejects the token, before
// logging in again, so the rejected token is removed from the keyring too.
func (c *tokenCache) SetToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	c.loaded = true
	if c.keyring == nil {
		return
	}
	if err := c.delete(); err != nil && !errors.Is(err, keyring.ErrNotFound) {
		c.logger.Warn("could not remove the Trino token from the keyring", "error", err)
	}
	if token == "" {
		return
	}
	if err := c.store(token); err != nil {
		c.logger.Warn("could not save the Trino token to the keyring; it will only be reused within this process", "error", err)
	}
}

// store saves token under the key, or in chunks when it is too big for one
// keyring entry, with the key holding the number of chunks. Big tokens are
// not tried in one entry first: on macOS, go-keyring leaves the security
// process it started running when it rejects them. The number of chunks is
// saved last, so that other processes do not read chunks that are not saved
// yet.
func (c *tokenCache) store(token string) error {
	if len(token) <= keyringChunkSize {
		return c.keyring.Set(keyringService, c.key, token)
	}
	n := (len(token) + keyringChunkSize - 1) / keyringChunkSize
	for i := range n {
		chunk := token[i*keyringChunkSize : min((i+1)*keyringChunkSize, len(token))]
		if err := c.keyring.Set(keyringService, chunkKey(c.key, i), chunk); err != nil {
			return err
		}
	}
	return c.keyring.Set(keyringService, c.key, keyringChunksPrefix+strconv.Itoa(n))
}

func (c *tokenCache) load() (string, error) {
	value, err := c.keyring.Get(keyringService, c.key)
	if err != nil {
		return "", err
	}
	n, chunked, err := chunkCount(value)
	if err != nil {
		return "", err
	}
	if !chunked {
		return value, nil
	}
	var b strings.Builder
	for i := range n {
		chunk, err := c.keyring.Get(keyringService, chunkKey(c.key, i))
		if err != nil {
			// Not wrapped: a missing chunk is a corrupt token, which is
			// reported, rather than no token.
			return "", fmt.Errorf("reading token chunk %d of %d: %v", i+1, n, err)
		}
		b.WriteString(chunk)
	}
	return b.String(), nil
}

// delete removes the token, and its chunks until one is missing, which also
// removes the chunks that an interrupted store left without their number.
func (c *tokenCache) delete() error {
	for i := 0; c.keyring.Delete(keyringService, chunkKey(c.key, i)) == nil; i++ {
	}
	return c.keyring.Delete(keyringService, c.key)
}

func chunkKey(key string, i int) string {
	return key + "#" + strconv.Itoa(i)
}

func chunkCount(value string) (n int, chunked bool, err error) {
	count, ok := strings.CutPrefix(value, keyringChunksPrefix)
	if !ok {
		return 0, false, nil
	}
	n, err = strconv.Atoi(count)
	if err != nil || n < 1 {
		return 0, true, fmt.Errorf("invalid token chunk count %q", count)
	}
	return n, true, nil
}

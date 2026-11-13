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
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
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
	// tokenCacheKeyring keeps the token in memory for the process and in the
	// OS keyring, so that later processes reuse it. This is the default.
	tokenCacheKeyring = "keyring"
	// tokenCacheMemory keeps the token in memory for the process only.
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

// systemKeyring is replaced in tests.
var systemKeyring keyringBackend = osKeyring{}

// tokenCaches holds one cache per server and user, so that every database
// in the process reuses the same token instead of logging in again.
var tokenCaches sync.Map

// tokenCache implements trino.TokenCache. The token is kept in memory, since
// the client reads it for every request, and written through to the keyring
// when persist is set.
type tokenCache struct {
	key     string
	persist bool
	logger  *slog.Logger

	mu     sync.Mutex
	token  string
	loaded bool
}

var _ trino.TokenCache = (*tokenCache)(nil)

// externalAuthOptions returns whether opts log in through external
// authentication, and where the token is kept.
func externalAuthOptions(opts map[string]string) (externalAuth bool, tokenCache string, err error) {
	tokenCache = cmp.Or(opts[optionTokenCache], tokenCacheKeyring)
	if tokenCache != tokenCacheKeyring && tokenCache != tokenCacheMemory {
		return false, "", fmt.Errorf("invalid value for %s: %q (expected %q or %q)", optionTokenCache, tokenCache, tokenCacheKeyring, tokenCacheMemory)
	}
	value, ok := opts[optionExternalAuthentication]
	if !ok {
		u, err := driverbase.SafeParseURL(opts[adbc.OptionKeyURI])
		if err != nil {
			return false, "", err
		}
		value = cmp.Or(u.Query().Get("externalAuthentication"), "false")
	}
	externalAuth, err = strconv.ParseBool(value)
	if err != nil {
		return false, "", fmt.Errorf("invalid value for %s: %q", optionExternalAuthentication, value)
	}
	return externalAuth, tokenCache, nil
}

// getTokenCache returns the process-wide cache for the server and user of
// serverURI, which also keeps the token in the OS keyring when persist is set.
func getTokenCache(serverURI string, persist bool, logger *slog.Logger) (*tokenCache, error) {
	key, err := tokenCacheKey(serverURI)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	// Memory and keyring caches for the same server are kept apart, so that
	// a database that opted out of the keyring never writes to it.
	cache, _ := tokenCaches.LoadOrStore(
		fmt.Sprintf("persist=%t|%s", persist, key),
		&tokenCache{key: key, persist: persist, logger: logger},
	)
	return cache.(*tokenCache), nil
}

// tokenCacheKey identifies the server and user a token belongs to.
func tokenCacheKey(serverURI string) (string, error) {
	u, err := url.Parse(serverURI)
	if err != nil {
		return "", fmt.Errorf("could not parse Trino server URI: %w", err)
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[strings.ToLower(u.Scheme)]
	}
	key := strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
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
		if c.persist {
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
	if !c.persist {
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
// keyring entry, with the key holding the number of chunks.
func (c *tokenCache) store(token string) error {
	err := systemKeyring.Set(keyringService, c.key, token)
	if !errors.Is(err, keyring.ErrSetDataTooBig) {
		return err
	}
	n := 0
	for start := 0; start < len(token); start += keyringChunkSize {
		chunk := token[start:min(start+keyringChunkSize, len(token))]
		if err := systemKeyring.Set(keyringService, chunkKey(c.key, n), chunk); err != nil {
			return err
		}
		n++
	}
	return systemKeyring.Set(keyringService, c.key, keyringChunksPrefix+strconv.Itoa(n))
}

func (c *tokenCache) load() (string, error) {
	value, err := systemKeyring.Get(keyringService, c.key)
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
		chunk, err := systemKeyring.Get(keyringService, chunkKey(c.key, i))
		if err != nil {
			// Not wrapped: a missing chunk is a corrupt token, which is
			// reported, rather than no token.
			return "", fmt.Errorf("reading token chunk %d of %d: %v", i+1, n, err)
		}
		b.WriteString(chunk)
	}
	return b.String(), nil
}

func (c *tokenCache) delete() error {
	value, err := systemKeyring.Get(keyringService, c.key)
	if err != nil {
		return err
	}
	if n, chunked, err := chunkCount(value); err == nil && chunked {
		for i := range n {
			_ = systemKeyring.Delete(keyringService, chunkKey(c.key, i))
		}
	}
	return systemKeyring.Delete(keyringService, c.key)
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

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
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
	"github.com/zalando/go-keyring"
)

// fakeKeyring is an in-memory keyring that rejects values over maxSize, like
// the Windows Credential Manager does.
type fakeKeyring struct {
	mu      sync.Mutex
	entries map[string]string
	maxSize int
	err     error
}

func (k *fakeKeyring) Set(service, user, password string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err != nil {
		return k.err
	}
	if k.maxSize > 0 && len(password) > k.maxSize {
		return keyring.ErrSetDataTooBig
	}
	k.entries[service+"/"+user] = password
	return nil
}

func (k *fakeKeyring) Get(service, user string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err != nil {
		return "", k.err
	}
	v, ok := k.entries[service+"/"+user]
	if !ok {
		return "", keyring.ErrNotFound
	}
	return v, nil
}

func (k *fakeKeyring) Delete(service, user string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err != nil {
		return k.err
	}
	if _, ok := k.entries[service+"/"+user]; !ok {
		return keyring.ErrNotFound
	}
	delete(k.entries, service+"/"+user)
	return nil
}

// useFakeKeyring replaces the OS keyring and the process-wide caches for the
// duration of the test.
func useFakeKeyring(t *testing.T) *fakeKeyring {
	t.Helper()
	k := &fakeKeyring{entries: map[string]string{}}
	saved := systemKeyring
	systemKeyring = k
	resetTokenCaches()
	t.Cleanup(func() {
		systemKeyring = saved
		resetTokenCaches()
	})
	return k
}

// resetTokenCaches forgets the in-memory tokens, as a new process would.
func resetTokenCaches() {
	tokenCaches.Clear()
}

func testConfig(t *testing.T, serverURI string) *trino.Config {
	t.Helper()
	return &trino.Config{ServerURI: serverURI}
}

func TestTokenCacheKey(t *testing.T) {
	for _, tc := range []struct{ uri, key string }{
		{"https://trino.example.com", "https://trino.example.com:443"},
		{"https://Trino.Example.com:443", "https://trino.example.com:443"},
		{"https://alice@trino.example.com:8443", "alice@https://trino.example.com:8443"},
		{"https://[::1]:8443", "https://[::1]:8443"},
	} {
		key, err := tokenCacheKey(tc.uri)
		require.NoError(t, err)
		assert.Equal(t, tc.key, key, tc.uri)
	}
}

func TestTokenCacheSharedPerServerAndUser(t *testing.T) {
	useFakeKeyring(t)

	a, err := getTokenCache(testConfig(t, "https://alice@trino.example.com"), "", nil)
	require.NoError(t, err)
	b, err := getTokenCache(testConfig(t, "https://alice@trino.example.com:443"), TokenCacheKeyring, nil)
	require.NoError(t, err)
	other, err := getTokenCache(testConfig(t, "https://bob@trino.example.com"), "", nil)
	require.NoError(t, err)

	assert.Same(t, a, b)
	assert.NotSame(t, a, other)

	a.SetToken("alice-token")
	assert.Equal(t, "alice-token", b.Token())
	assert.Equal(t, "", other.Token())
}

func TestTokenCacheKeyringPersistsAcrossProcesses(t *testing.T) {
	k := useFakeKeyring(t)
	cfg := testConfig(t, "https://trino.example.com")

	cache, err := getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
	cache.SetToken("token-1")
	assert.Equal(t, "token-1", k.entries[keyringService+"/https://trino.example.com:443"])

	resetTokenCaches()
	cache, err = getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "token-1", cache.Token())

	// The client clears a rejected token before logging in again.
	cache.SetToken("")
	assert.Empty(t, k.entries)
	resetTokenCaches()
	cache, err = getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
}

func TestTokenCacheKeyringChunksLargeTokens(t *testing.T) {
	k := useFakeKeyring(t)
	k.maxSize = 2560
	cfg := testConfig(t, "https://trino.example.com")
	token := strings.Repeat("abcdefghij", 450) // 4500 bytes, like a large JWT

	cache, err := getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	cache.SetToken(token)
	assert.Len(t, k.entries, 6, "5 chunks and the chunk count")

	resetTokenCaches()
	cache, err = getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	assert.Equal(t, token, cache.Token())

	cache.SetToken("small")
	assert.Equal(t, map[string]string{keyringService + "/https://trino.example.com:443": "small"}, k.entries)
}

func TestTokenCacheMemoryDoesNotUseKeyring(t *testing.T) {
	k := useFakeKeyring(t)
	cfg := testConfig(t, "https://trino.example.com")

	cache, err := getTokenCache(cfg, TokenCacheMemory, nil)
	require.NoError(t, err)
	cache.SetToken("token-1")
	assert.Empty(t, k.entries)

	again, err := getTokenCache(cfg, TokenCacheMemory, nil)
	require.NoError(t, err)
	assert.Equal(t, "token-1", again.Token())

	keyringCache, err := getTokenCache(cfg, TokenCacheKeyring, nil)
	require.NoError(t, err)
	assert.NotSame(t, cache, keyringCache)
}

func TestTokenCacheKeyringUnavailable(t *testing.T) {
	k := useFakeKeyring(t)
	k.err = errors.New("no secret service")

	cache, err := getTokenCache(testConfig(t, "https://trino.example.com"), "", nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
	cache.SetToken("token-1")
	assert.Equal(t, "token-1", cache.Token())
}

func TestTokenCacheInvalidOption(t *testing.T) {
	useFakeKeyring(t)
	_, err := NewTrinoDBFactory().CreateDB(context.Background(), "trino", map[string]string{
		adbc.OptionKeyURI: "trino://trino.example.com/memory?externalAuthentication=true",
		OptionTokenCache:  "disk",
	}, nil)
	require.ErrorContains(t, err, `invalid value for trino.token_cache: "disk"`)
}

// fakeOAuthCoordinator answers queries only with its token, and challenges
// other requests with Trino's external authentication flow without a
// redirect URL, so that no browser is opened.
func fakeOAuthCoordinator(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var logins atomic.Int32
	const token = "oauth-token"
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oauth2/token/1" && r.Method == http.MethodGet:
			logins.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"token":%q}`, token)
		case r.URL.Path == "/oauth2/token/1" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Header.Get("Authorization") != "Bearer "+token:
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer x_token_server="%s/oauth2/token/1"`, server.URL))
			w.WriteHeader(http.StatusUnauthorized)
		case r.URL.Path == "/v1/statement":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"id":"q1","nextUri":"%s/v1/statement/q1/1","stats":{"state":"QUEUED"}}`, server.URL)
		case r.URL.Path == "/v1/statement/q1/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"id":"q1","stats":{"state":"FINISHED"},"columns":[{"name":"_col0","type":"integer","typeSignature":{"rawType":"integer","arguments":[]}}],"data":[[1]]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, &logins
}

func queryOnce(t *testing.T, serverURL string, opts map[string]string) {
	t.Helper()
	all := map[string]string{
		adbc.OptionKeyURI: strings.Replace(serverURL, "https://", "trino://", 1) + "/memory?externalAuthentication=true&SSLVerification=NONE",
	}
	maps.Copy(all, opts)
	db, err := NewTrinoDBFactory().CreateDB(context.Background(), "trino", all, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), "SELECT 1").Scan(&n))
	assert.Equal(t, 1, n)
}

func TestExternalAuthenticationReusesTokenAcrossDatabases(t *testing.T) {
	useFakeKeyring(t)
	server, logins := fakeOAuthCoordinator(t)

	queryOnce(t, server.URL, nil)
	queryOnce(t, server.URL, nil)
	assert.Equal(t, int32(1), logins.Load(), "databases in one process share the token")

	resetTokenCaches()
	queryOnce(t, server.URL, nil)
	assert.Equal(t, int32(1), logins.Load(), "a new process reuses the token from the keyring")
}

func TestExternalAuthenticationMemoryTokenCache(t *testing.T) {
	k := useFakeKeyring(t)
	server, logins := fakeOAuthCoordinator(t)
	opts := map[string]string{OptionTokenCache: TokenCacheMemory}

	queryOnce(t, server.URL, opts)
	queryOnce(t, server.URL, opts)
	assert.Equal(t, int32(1), logins.Load())
	assert.Empty(t, k.entries)

	resetTokenCaches()
	queryOnce(t, server.URL, opts)
	assert.Equal(t, int32(2), logins.Load(), "the token does not outlive the process")
}

func TestExternalAuthenticationReplacesRejectedKeyringToken(t *testing.T) {
	k := useFakeKeyring(t)
	server, logins := fakeOAuthCoordinator(t)
	key, err := tokenCacheKey(server.URL)
	require.NoError(t, err)
	k.entries[keyringService+"/"+key] = "expired-token"

	queryOnce(t, server.URL, nil)
	assert.Equal(t, int32(1), logins.Load())
	assert.Equal(t, "oauth-token", k.entries[keyringService+"/"+key])
}

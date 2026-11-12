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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
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

// bufferLogger returns a logger whose output the test can inspect.
func bufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestTokenCacheInvalidServerURI(t *testing.T) {
	useFakeKeyring(t)
	_, err := getTokenCache(testConfig(t, "https://%zz"), "", nil)
	require.ErrorContains(t, err, "could not parse Trino server URI")
}

func TestTokenCacheKeyringChunkWriteFails(t *testing.T) {
	k := useFakeKeyring(t)
	k.maxSize = 10 // smaller than a chunk, so the chunks cannot be saved either
	logger, logs := bufferLogger()

	cache, err := getTokenCache(testConfig(t, "https://trino.example.com"), "", logger)
	require.NoError(t, err)
	cache.SetToken("a-token-over-ten-bytes")
	assert.Equal(t, "a-token-over-ten-bytes", cache.Token(), "the token is still kept in memory")
	assert.NotContains(t, k.entries, keyringService+"/https://trino.example.com:443")
	assert.Contains(t, logs.String(), "could not save the Trino token to the keyring")
}

func TestTokenCacheKeyringCorruptChunks(t *testing.T) {
	const key = keyringService + "/https://trino.example.com:443"
	for _, tc := range []struct {
		name    string
		entries map[string]string
		warning string
	}{
		{
			name:    "missing chunk",
			entries: map[string]string{key: keyringChunksPrefix + "2", key + "#0": "first-half"},
			warning: "reading token chunk 2 of 2",
		},
		{
			name:    "invalid chunk count",
			entries: map[string]string{key: keyringChunksPrefix + "x"},
			warning: "invalid token chunk count",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := useFakeKeyring(t)
			maps.Copy(k.entries, tc.entries)
			logger, logs := bufferLogger()

			cache, err := getTokenCache(testConfig(t, "https://trino.example.com"), "", logger)
			require.NoError(t, err)
			assert.Equal(t, "", cache.Token(), "a corrupt token is not used, so the user logs in again")
			assert.Contains(t, logs.String(), tc.warning)

			cache.SetToken("new-token")
			assert.Equal(t, "new-token", k.entries[key])
		})
	}
}

// useOSKeyring uses the OS keyring for the duration of the test, and skips
// it when there is none (e.g. Linux without a Secret Service).
func useOSKeyring(t *testing.T) {
	t.Helper()
	probe := "probe-" + rand.Text()
	if err := keyring.Set(keyringService, probe, "probe"); err != nil {
		t.Skipf("no OS keyring available: %v", err)
	}
	require.NoError(t, keyring.Delete(keyringService, probe))
	saved := systemKeyring
	systemKeyring = osKeyring{}
	resetTokenCaches()
	t.Cleanup(func() {
		systemKeyring = saved
		resetTokenCaches()
	})
}

// forgetOSKeyringToken removes what the tests saved in the OS keyring for key.
func forgetOSKeyringToken(t *testing.T, key string) {
	t.Helper()
	t.Cleanup(func() {
		_ = (&tokenCache{key: key}).delete()
	})
}

func TestTokenCacheOSKeyring(t *testing.T) {
	useOSKeyring(t)
	// A server of its own, so the test never touches a real token.
	cfg := testConfig(t, "https://"+strings.ToLower(rand.Text())+".adbc-test.invalid")
	key, err := tokenCacheKey(cfg.ServerURI)
	require.NoError(t, err)
	forgetOSKeyringToken(t, key)

	// 4500 bytes, over the Windows and macOS limits, so it is saved in chunks
	// there.
	for _, token := range []string{"small-token", strings.Repeat("abcdefghij", 450)} {
		cache, err := getTokenCache(cfg, "", nil)
		require.NoError(t, err)
		cache.SetToken(token)

		resetTokenCaches()
		cache, err = getTokenCache(cfg, "", nil)
		require.NoError(t, err)
		assert.Equal(t, token, cache.Token(), "a new process reads the token from the OS keyring")
	}

	cache, err := getTokenCache(cfg, "", nil)
	require.NoError(t, err)
	cache.SetToken("")
	_, err = keyring.Get(keyringService, key)
	require.ErrorIs(t, err, keyring.ErrNotFound)
	for i := range 5 {
		_, err = keyring.Get(keyringService, chunkKey(key, i))
		require.ErrorIs(t, err, keyring.ErrNotFound, "chunk %d", i)
	}
}

// trinoOAuth2DSN returns the URI of the local Trino with OAuth 2.0 (see
// compose.yaml), and makes the driver log in through it without a browser.
// It returns the number of logins.
func trinoOAuth2DSN(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	dsn := os.Getenv("TRINO_OAUTH2_DSN")
	if dsn == "" {
		t.Skip("Set TRINO_OAUTH2_DSN environment variable for OAuth 2.0 tests")
	}
	ca, err := os.ReadFile(os.Getenv("TRINO_SSL_CERT_PATH"))
	require.NoError(t, err, "TRINO_SSL_CERT_PATH must point to the CA of the local Trino")
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(ca))

	var logins atomic.Int32
	saved := externalAuthRedirectHandler
	externalAuthRedirectHandler = func(ctx context.Context, redirectURL *url.URL) error {
		logins.Add(1)
		// Act as the browser: follow the redirects to the identity provider,
		// which logs in without a login page, and back to Trino.
		jar, err := cookiejar.New(nil)
		if err != nil {
			return err
		}
		browser := &http.Client{Jar: jar, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, redirectURL.String(), nil)
		if err != nil {
			return err
		}
		resp, err := browser.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("login ended with %s at %s", resp.Status, resp.Request.URL)
		}
		return nil
	}
	t.Cleanup(func() { externalAuthRedirectHandler = saved })
	return dsn, &logins
}

// queryCurrentUser opens a database and runs a query, as a new connection
// of a client (e.g. dbt) does.
func queryCurrentUser(t *testing.T, dsn string, opts map[string]string) {
	t.Helper()
	all := map[string]string{adbc.OptionKeyURI: dsn}
	maps.Copy(all, opts)
	db, err := NewTrinoDBFactory().CreateDB(context.Background(), "trino", all, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var user string
	require.NoError(t, db.QueryRowContext(context.Background(), "SELECT current_user").Scan(&user))
	assert.Equal(t, "oauth-user", user, "the user of the identity provider")
}

func TestTrinoOAuth2ReusesToken(t *testing.T) {
	dsn, logins := trinoOAuth2DSN(t)
	k := useFakeKeyring(t)

	queryCurrentUser(t, dsn, nil)
	queryCurrentUser(t, dsn, nil)
	assert.Equal(t, int32(1), logins.Load(), "databases in one process share the token")
	assert.Len(t, k.entries, 1)

	resetTokenCaches()
	queryCurrentUser(t, dsn, nil)
	assert.Equal(t, int32(1), logins.Load(), "a new process reuses the token from the keyring")
}

func TestTrinoOAuth2MemoryTokenCache(t *testing.T) {
	dsn, logins := trinoOAuth2DSN(t)
	k := useFakeKeyring(t)
	opts := map[string]string{OptionTokenCache: TokenCacheMemory}

	queryCurrentUser(t, dsn, opts)
	queryCurrentUser(t, dsn, opts)
	assert.Equal(t, int32(1), logins.Load())
	assert.Empty(t, k.entries)

	resetTokenCaches()
	queryCurrentUser(t, dsn, opts)
	assert.Equal(t, int32(2), logins.Load(), "the token does not outlive the process")
}

func TestTrinoOAuth2ReplacesRejectedToken(t *testing.T) {
	dsn, logins := trinoOAuth2DSN(t)
	k := useFakeKeyring(t)
	cfg, err := trino.ParseDSN(dsn)
	require.NoError(t, err)
	key, err := tokenCacheKey(cfg.ServerURI)
	require.NoError(t, err)
	k.entries[keyringService+"/"+key] = "expired-token"

	queryCurrentUser(t, dsn, nil)
	assert.Equal(t, int32(1), logins.Load())
	assert.NotEqual(t, "expired-token", k.entries[keyringService+"/"+key])
	assert.NotEmpty(t, k.entries[keyringService+"/"+key])
}

func TestTrinoOAuth2OSKeyring(t *testing.T) {
	dsn, logins := trinoOAuth2DSN(t)
	useOSKeyring(t)
	cfg, err := trino.ParseDSN(dsn)
	require.NoError(t, err)
	key, err := tokenCacheKey(cfg.ServerURI)
	require.NoError(t, err)
	// Start from no token, and do not leave the test's token behind.
	_ = (&tokenCache{key: key}).delete()
	forgetOSKeyringToken(t, key)

	queryCurrentUser(t, dsn, nil)
	assert.Equal(t, int32(1), logins.Load())

	resetTokenCaches()
	queryCurrentUser(t, dsn, nil)
	assert.Equal(t, int32(1), logins.Load(), "a new process reuses the token from the OS keyring")
}

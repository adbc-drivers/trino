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
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	a, err := getTokenCache("https://alice@trino.example.com", true, nil)
	require.NoError(t, err)
	b, err := getTokenCache("https://alice@trino.example.com:443", true, nil)
	require.NoError(t, err)
	other, err := getTokenCache("https://bob@trino.example.com", true, nil)
	require.NoError(t, err)

	assert.Same(t, a, b)
	assert.NotSame(t, a, other)

	a.SetToken("alice-token")
	assert.Equal(t, "alice-token", b.Token())
	assert.Equal(t, "", other.Token())
}

func TestTokenCacheKeyringPersistsAcrossProcesses(t *testing.T) {
	k := useFakeKeyring(t)
	uri := "https://trino.example.com"

	cache, err := getTokenCache(uri, true, nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
	cache.SetToken("token-1")
	assert.Equal(t, "token-1", k.entries[keyringService+"/https://trino.example.com:443"])

	resetTokenCaches()
	cache, err = getTokenCache(uri, true, nil)
	require.NoError(t, err)
	assert.Equal(t, "token-1", cache.Token())

	// The client clears a rejected token before logging in again.
	cache.SetToken("")
	assert.Empty(t, k.entries)
	resetTokenCaches()
	cache, err = getTokenCache(uri, true, nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
}

func TestTokenCacheKeyringChunksLargeTokens(t *testing.T) {
	k := useFakeKeyring(t)
	k.maxSize = 2560
	uri := "https://trino.example.com"
	token := strings.Repeat("abcdefghij", 450) // 4500 bytes, like a large JWT

	cache, err := getTokenCache(uri, true, nil)
	require.NoError(t, err)
	cache.SetToken(token)
	assert.Len(t, k.entries, 6, "5 chunks and the chunk count")

	resetTokenCaches()
	cache, err = getTokenCache(uri, true, nil)
	require.NoError(t, err)
	assert.Equal(t, token, cache.Token())

	cache.SetToken("small")
	assert.Equal(t, map[string]string{keyringService + "/https://trino.example.com:443": "small"}, k.entries)
}

func TestTokenCacheMemoryDoesNotUseKeyring(t *testing.T) {
	k := useFakeKeyring(t)
	uri := "https://trino.example.com"

	cache, err := getTokenCache(uri, false, nil)
	require.NoError(t, err)
	cache.SetToken("token-1")
	assert.Empty(t, k.entries)

	again, err := getTokenCache(uri, false, nil)
	require.NoError(t, err)
	assert.Equal(t, "token-1", again.Token())

	keyringCache, err := getTokenCache(uri, true, nil)
	require.NoError(t, err)
	assert.NotSame(t, cache, keyringCache)
}

func TestTokenCacheKeyringUnavailable(t *testing.T) {
	k := useFakeKeyring(t)
	k.err = errors.New("no secret service")

	cache, err := getTokenCache("https://trino.example.com", true, nil)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
	cache.SetToken("token-1")
	assert.Equal(t, "token-1", cache.Token())
}

func TestExternalAuthOptions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		opts         map[string]string
		externalAuth bool
		tokenCache   string
		err          string
	}{
		{name: "defaults", opts: map[string]string{adbc.OptionKeyURI: "trino://trino.example.com/memory"}, tokenCache: tokenCacheKeyring},
		{name: "URI parameter", opts: map[string]string{adbc.OptionKeyURI: "https://trino.example.com?externalAuthentication=true"}, externalAuth: true, tokenCache: tokenCacheKeyring},
		{name: "option", opts: map[string]string{adbc.OptionKeyURI: "trino://trino.example.com/memory", optionExternalAuthentication: "true", optionTokenCache: tokenCacheMemory}, externalAuth: true, tokenCache: tokenCacheMemory},
		{name: "option overrides URI", opts: map[string]string{adbc.OptionKeyURI: "trino://trino.example.com/memory?externalAuthentication=true", optionExternalAuthentication: "false"}, tokenCache: tokenCacheKeyring},
		{name: "invalid option", opts: map[string]string{optionExternalAuthentication: "maybe"}, err: `invalid value for trino.external_authentication: "maybe"`},
		{name: "invalid URI parameter", opts: map[string]string{adbc.OptionKeyURI: "trino://trino.example.com/memory?externalAuthentication=maybe"}, err: `invalid value for trino.external_authentication: "maybe"`},
		{name: "invalid URI", opts: map[string]string{adbc.OptionKeyURI: "trino://trino.example.com:port/memory"}, err: "invalid port"},
		{name: "invalid token cache", opts: map[string]string{optionTokenCache: "disk"}, err: `invalid value for trino.token_cache: "disk"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			externalAuth, tokenCache, err := externalAuthOptions(tc.opts)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.externalAuth, externalAuth)
			assert.Equal(t, tc.tokenCache, tokenCache)
		})
	}
}

// bufferLogger returns a logger whose output the test can inspect.
func bufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestTokenCacheInvalidServerURI(t *testing.T) {
	useFakeKeyring(t)
	_, err := getTokenCache("https://%zz", true, nil)
	require.ErrorContains(t, err, "could not parse Trino server URI")
}

func TestTokenCacheKeyringChunkWriteFails(t *testing.T) {
	k := useFakeKeyring(t)
	k.maxSize = 10 // smaller than a chunk, so the chunks cannot be saved either
	logger, logs := bufferLogger()

	cache, err := getTokenCache("https://trino.example.com", true, logger)
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

			cache, err := getTokenCache("https://trino.example.com", true, logger)
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
	uri := "https://" + strings.ToLower(rand.Text()) + ".adbc-test.invalid"
	key, err := tokenCacheKey(uri)
	require.NoError(t, err)
	forgetOSKeyringToken(t, key)

	// 4500 bytes, over the Windows and macOS limits, so it is saved in chunks
	// there.
	for _, token := range []string{"small-token", strings.Repeat("abcdefghij", 450)} {
		cache, err := getTokenCache(uri, true, nil)
		require.NoError(t, err)
		cache.SetToken(token)

		resetTokenCaches()
		cache, err = getTokenCache(uri, true, nil)
		require.NoError(t, err)
		assert.Equal(t, token, cache.Token(), "a new process reads the token from the OS keyring")
	}

	cache, err := getTokenCache(uri, true, nil)
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
	opts := map[string]string{optionTokenCache: tokenCacheMemory}

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

// openDatabase opens a database through the ADBC driver.
func openDatabase(t *testing.T, opts map[string]string) adbc.DatabaseWithContext {
	t.Helper()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	return db
}

// assertDatabaseOptions checks the options a database reports, and that they
// cannot be changed after it is created.
func assertDatabaseOptions(t *testing.T, db adbc.DatabaseWithContext, externalAuth, tokenCache string) {
	t.Helper()
	options := db.(adbc.GetSetOptionsWithContext)
	for key, expected := range map[string]string{optionExternalAuthentication: externalAuth, optionTokenCache: tokenCache} {
		value, err := options.GetOption(context.Background(), key)
		require.NoError(t, err)
		assert.Equal(t, expected, value, key)
	}
	var adbcErr adbc.Error
	require.ErrorAs(t, db.SetOptions(context.Background(), map[string]string{optionTokenCache: tokenCacheMemory}), &adbcErr)
	assert.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
	_, err := options.GetOption(context.Background(), "trino.unknown")
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusNotFound, adbcErr.Code)
}

func TestDatabaseInvalidOption(t *testing.T) {
	_, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
		adbc.OptionKeyURI: "trino://trino.example.com/memory",
		optionTokenCache:  "disk",
	})
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	assert.ErrorContains(t, err, `invalid value for trino.token_cache: "disk"`)
}

func TestDatabaseOptionsDefaults(t *testing.T) {
	dsn := os.Getenv("TRINO_DSN")
	if dsn == "" {
		t.Skip("Set TRINO_DSN environment variable")
	}
	db := openDatabase(t, map[string]string{adbc.OptionKeyURI: dsn})
	assertDatabaseOptions(t, db, "false", tokenCacheKeyring)
}

func TestTrinoOAuth2Option(t *testing.T) {
	dsn, logins := trinoOAuth2DSN(t)
	useFakeKeyring(t)
	// The option enables external authentication without the URI parameter.
	opts := map[string]string{
		adbc.OptionKeyURI:            strings.Replace(dsn, "&externalAuthentication=true", "", 1),
		optionExternalAuthentication: "true",
		optionTokenCache:             tokenCacheMemory,
	}

	assertDatabaseOptions(t, openDatabase(t, opts), "true", tokenCacheMemory)
	queryCurrentUser(t, opts[adbc.OptionKeyURI], opts)
	assert.Equal(t, int32(1), logins.Load())
}

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
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
	"github.com/zalando/go-keyring"
)

// memoryKeyring is a keyring kept in memory, for the tests that do not need
// the OS keyring. err makes every operation fail, as when there is no keyring.
type memoryKeyring struct {
	mu      sync.Mutex
	entries map[string]string
	err     error
}

func newMemoryKeyring() *memoryKeyring {
	return &memoryKeyring{entries: map[string]string{}}
}

func (k *memoryKeyring) Set(service, user, password string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err != nil {
		return k.err
	}
	k.entries[service+"/"+user] = password
	return nil
}

func (k *memoryKeyring) Get(service, user string) (string, error) {
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

func (k *memoryKeyring) Delete(service, user string) error {
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

// bufferLogger returns a logger whose output the test can inspect.
func bufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
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
	_, err := (&TrinoDBFactory{}).tokenCache("https://%zz", tokenCacheMemory, nil)
	require.ErrorContains(t, err, "could not parse Trino server URI")
}

func TestExternalAuthOptions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		opts         map[string]string
		uriValue     bool
		externalAuth bool
		tokenCache   string
		err          string
	}{
		{name: "defaults", tokenCache: tokenCacheKeyring},
		{name: "URI parameter", uriValue: true, externalAuth: true, tokenCache: tokenCacheKeyring},
		{name: "option", opts: map[string]string{optionExternalAuthentication: "true", optionTokenCache: tokenCacheMemory}, externalAuth: true, tokenCache: tokenCacheMemory},
		{name: "option overrides URI parameter", opts: map[string]string{optionExternalAuthentication: "false"}, uriValue: true, tokenCache: tokenCacheKeyring},
		{name: "invalid option", opts: map[string]string{optionExternalAuthentication: "maybe"}, err: `invalid value for trino.external_authentication: "maybe"`},
		{name: "invalid token cache", opts: map[string]string{optionTokenCache: "disk"}, err: `invalid value for trino.token_cache: "disk"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			externalAuth, tokenCache, err := externalAuthOptions(tc.opts, tc.uriValue)
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

// URIs without a scheme, which BuildTrinoDSN completes, also work with
// external authentication.
func TestCreateDBURIWithoutScheme(t *testing.T) {
	for _, uri := range []string{"10.0.0.1:8443?catalog=memory", "alice@trino.example.com:8443?externalAuthentication=true"} {
		db, err := NewTrinoDBFactory().CreateDB(context.Background(), "trino", map[string]string{adbc.OptionKeyURI: uri}, nil)
		require.NoError(t, err, uri)
		require.NoError(t, db.Close())
	}
}

func TestTokenCacheKeyringChunksLargeTokens(t *testing.T) {
	k := newMemoryKeyring()
	const uri = "https://trino.example.com"
	const key = keyringService + "/https://trino.example.com:443"
	token := strings.Repeat("abcdefghij", 450) // 4500 bytes, like an Entra ID token

	cache, err := newTokenCache(uri, k, nil)
	require.NoError(t, err)
	cache.SetToken(token)
	assert.Equal(t, keyringChunksPrefix+"5", k.entries[key])
	assert.Len(t, k.entries, 6, "5 chunks and their count")

	// A new process reads the chunks back.
	cache, err = newTokenCache(uri, k, nil)
	require.NoError(t, err)
	assert.Equal(t, token, cache.Token())

	cache.SetToken("small-token")
	assert.Equal(t, map[string]string{key: "small-token"}, k.entries, "the chunks are removed")
}

func TestTokenCacheKeyringUnavailable(t *testing.T) {
	k := newMemoryKeyring()
	k.err = errors.New("no secret service")
	logger, logs := bufferLogger()

	cache, err := newTokenCache("https://trino.example.com", k, logger)
	require.NoError(t, err)
	assert.Equal(t, "", cache.Token())
	token := strings.Repeat("abcdefghij", 450)
	cache.SetToken(token)
	assert.Equal(t, token, cache.Token(), "the token is still reused within the process")
	for _, warning := range []string{
		"could not read the Trino token from the keyring",
		"could not remove the Trino token from the keyring",
		"could not save the Trino token to the keyring",
	} {
		assert.Contains(t, logs.String(), warning)
	}
}

// A process that stops while saving a token in chunks leaves them incomplete;
// they are not used, and they are removed with the next token.
func TestTokenCacheKeyringIncompleteToken(t *testing.T) {
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
			name:    "chunks without their number",
			entries: map[string]string{key + "#0": "first-half", key + "#1": "second-half"},
		},
		{
			name:    "invalid chunk count",
			entries: map[string]string{key: keyringChunksPrefix + "x"},
			warning: "invalid token chunk count",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newMemoryKeyring()
			maps.Copy(k.entries, tc.entries)
			logger, logs := bufferLogger()

			cache, err := newTokenCache("https://trino.example.com", k, logger)
			require.NoError(t, err)
			assert.Equal(t, "", cache.Token(), "the user logs in again")
			assert.Contains(t, logs.String(), tc.warning)

			cache.SetToken("new-token")
			assert.Equal(t, map[string]string{key: "new-token"}, k.entries)
		})
	}
}

// osKeyringOrSkip returns the OS keyring, and skips the test when there is
// none (e.g. Linux without a Secret Service).
func osKeyringOrSkip(t *testing.T) keyringBackend {
	t.Helper()
	probe := "probe-" + rand.Text()
	if err := keyring.Set(keyringService, probe, "probe"); err != nil {
		t.Skipf("no OS keyring available: %v", err)
	}
	require.NoError(t, keyring.Delete(keyringService, probe))
	return osKeyring{}
}

// forgetToken removes the token saved for key from k, now and after the test.
func forgetToken(t *testing.T, k keyringBackend, key string) {
	t.Helper()
	forget := func() { _ = (&tokenCache{key: key, keyring: k}).delete() }
	forget()
	t.Cleanup(forget)
}

func TestTokenCacheOSKeyring(t *testing.T) {
	k := osKeyringOrSkip(t)
	// A server of its own, so the test never touches a real token.
	uri := "https://" + strings.ToLower(rand.Text()) + ".adbc-test.invalid"
	key, err := tokenCacheKey(uri)
	require.NoError(t, err)
	forgetToken(t, k, key)

	for _, token := range []string{"small-token", strings.Repeat("abcdefghij", 450)} {
		cache, err := newTokenCache(uri, k, nil)
		require.NoError(t, err)
		cache.SetToken(token)

		cache, err = newTokenCache(uri, k, nil)
		require.NoError(t, err)
		assert.Equal(t, token, cache.Token(), "a new process reads the token from the OS keyring")
	}

	cache, err := newTokenCache(uri, k, nil)
	require.NoError(t, err)
	cache.SetToken("")
	for _, user := range []string{key, chunkKey(key, 0), chunkKey(key, 4)} {
		_, err = keyring.Get(keyringService, user)
		require.ErrorIs(t, err, keyring.ErrNotFound, user)
	}
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

// openDatabase opens a database through the ADBC driver.
func openDatabase(t *testing.T, f *TrinoDBFactory, opts map[string]string) adbc.DatabaseWithContext {
	t.Helper()
	db, err := newDriver(memory.DefaultAllocator, f).NewDatabaseWithContext(context.Background(), opts)
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

func TestDatabaseOptionsDefaults(t *testing.T) {
	dsn := os.Getenv("TRINO_DSN")
	if dsn == "" {
		t.Skip("Set TRINO_DSN environment variable")
	}
	db := openDatabase(t, NewTrinoDBFactory(), map[string]string{adbc.OptionKeyURI: dsn})
	assertDatabaseOptions(t, db, "false", tokenCacheKeyring)
}

// trinoOAuth2 returns the URI of the local Trino with OAuth 2.0 (see
// compose.yaml), and a redirect handler that logs in through it as a browser
// would, counting the logins.
func trinoOAuth2(t *testing.T) (string, trino.RedirectHandler, *atomic.Int32) {
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
	browser := func(ctx context.Context, redirectURL *url.URL) error {
		logins.Add(1)
		// Follow the redirects to the identity provider, which logs in
		// without a login page, and back to Trino.
		jar, err := cookiejar.New(nil)
		if err != nil {
			return err
		}
		client := &http.Client{Jar: jar, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, redirectURL.String(), nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("login ended with %s at %s", resp.Status, resp.Request.URL)
		}
		return nil
	}
	return dsn, browser, &logins
}

// queryCurrentUser opens a database and runs a query, as a new connection
// of a client (e.g. dbt) does. It returns an error rather than failing the
// test, so that it can run outside of the test's goroutine.
func queryCurrentUser(f *TrinoDBFactory, dsn string, opts map[string]string) error {
	all := map[string]string{adbc.OptionKeyURI: dsn}
	maps.Copy(all, opts)
	db, err := f.CreateDB(context.Background(), "trino", all, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var user string
	if err := db.QueryRowContext(context.Background(), "SELECT current_user").Scan(&user); err != nil {
		return err
	}
	if user != "oauth-user" {
		return fmt.Errorf("logged in as %q instead of the identity provider's user", user)
	}
	return nil
}

// tokenKey returns the token cache key of the server of dsn.
func tokenKey(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := trino.ParseDSN(dsn)
	require.NoError(t, err)
	key, err := tokenCacheKey(cfg.ServerURI)
	require.NoError(t, err)
	return key
}

func TestTrinoOAuth2ReusesToken(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	k := newMemoryKeyring()
	f := &TrinoDBFactory{keyring: k, redirect: browser}

	require.NoError(t, queryCurrentUser(f, dsn, nil))
	require.NoError(t, queryCurrentUser(f, dsn, nil))
	assert.Equal(t, int32(1), logins.Load(), "databases in one process share the token")
	assert.NotEmpty(t, k.entries[keyringService+"/"+tokenKey(t, dsn)])

	newProcess := &TrinoDBFactory{keyring: k, redirect: browser}
	require.NoError(t, queryCurrentUser(newProcess, dsn, nil))
	assert.Equal(t, int32(1), logins.Load(), "a new process reuses the token from the keyring")
}

func TestTrinoOAuth2ConcurrentLogin(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	// The login takes a while, as with a user in front of the browser, so
	// that all the databases ask for it before it ends.
	slowBrowser := func(ctx context.Context, redirectURL *url.URL) error {
		time.Sleep(time.Second)
		return browser(ctx, redirectURL)
	}
	f := &TrinoDBFactory{keyring: newMemoryKeyring(), redirect: slowBrowser}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { errs[i] = queryCurrentUser(f, dsn, nil) })
	}
	wg.Wait()
	require.NoError(t, errors.Join(errs...))
	assert.Equal(t, int32(1), logins.Load(), "databases connecting at once wait for one login")
}

func TestTrinoOAuth2MemoryTokenCache(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	k := newMemoryKeyring()
	opts := map[string]string{optionTokenCache: tokenCacheMemory}
	f := &TrinoDBFactory{keyring: k, redirect: browser}

	require.NoError(t, queryCurrentUser(f, dsn, opts))
	// Other options, hence another Connector, share the token in memory.
	otherOpts := map[string]string{optionTokenCache: tokenCacheMemory, "trino.http_header.X-Trino-Client-Tags": "adbc-test"}
	require.NoError(t, queryCurrentUser(f, dsn, otherOpts))
	assert.Equal(t, int32(1), logins.Load())
	assert.Empty(t, k.entries, "the keyring is not used")

	newProcess := &TrinoDBFactory{keyring: k, redirect: browser}
	require.NoError(t, queryCurrentUser(newProcess, dsn, opts))
	assert.Equal(t, int32(2), logins.Load(), "the token does not outlive the process")
}

func TestTrinoOAuth2ReplacesRejectedToken(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	k := newMemoryKeyring()
	key := keyringService + "/" + tokenKey(t, dsn)
	k.entries[key] = "expired-token"

	require.NoError(t, queryCurrentUser(&TrinoDBFactory{keyring: k, redirect: browser}, dsn, nil))
	assert.Equal(t, int32(1), logins.Load())
	assert.NotContains(t, []string{"", "expired-token"}, k.entries[key])
}

func TestTrinoOAuth2Option(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	f := &TrinoDBFactory{keyring: newMemoryKeyring(), redirect: browser}
	// The option enables external authentication without the URI parameter.
	uri := strings.Replace(dsn, "&externalAuthentication=true", "", 1)
	opts := map[string]string{
		adbc.OptionKeyURI:            uri,
		optionExternalAuthentication: "true",
		optionTokenCache:             tokenCacheMemory,
	}

	assertDatabaseOptions(t, openDatabase(t, f, opts), "true", tokenCacheMemory)
	require.NoError(t, queryCurrentUser(f, uri, opts))
	assert.Equal(t, int32(1), logins.Load())
}

func TestTrinoOAuth2OSKeyring(t *testing.T) {
	dsn, browser, logins := trinoOAuth2(t)
	k := osKeyringOrSkip(t)
	forgetToken(t, k, tokenKey(t, dsn))

	// Without a keyring, the factory uses the OS keyring.
	require.NoError(t, queryCurrentUser(&TrinoDBFactory{redirect: browser}, dsn, nil))
	newProcess := &TrinoDBFactory{redirect: browser}
	require.NoError(t, queryCurrentUser(newProcess, dsn, nil))
	assert.Equal(t, int32(1), logins.Load(), "a new process reuses the token from the OS keyring")
}

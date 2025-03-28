package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/prysm/v5/api/server/structs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/tele"
)

func TestPrysmClient_AddTrustedPeer(t *testing.T) {
	otel.SetTracerProvider(tele.NoopTracerProvider())

	maddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	require.NoError(t, err)

	pid, err := peer.Decode("16Uiu2HAmBBTgCRezbBY8LbdfDN5PXYi3C1hwdoXJ9DZAorsWs4NR")
	require.NoError(t, err)

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		respStatusCode int
		respBody       string
		expectErr      bool
	}{
		{
			name:           "success",
			method:         http.MethodPost,
			path:           "/prysm/node/trusted_peers",
			body:           fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
			respStatusCode: http.StatusOK,
			respBody:       "",
			expectErr:      false,
		},
		{
			name:           "error_json_unmarshal",
			method:         http.MethodPost,
			path:           "/prysm/node/trusted_peers",
			body:           fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
			respStatusCode: http.StatusBadRequest,
			respBody:       "{invalid_json}",
			expectErr:      true,
		},
		{
			name:           "invalid_response_status_code",
			method:         http.MethodPost,
			path:           "/prysm/node/trusted_peers",
			body:           fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
			respStatusCode: http.StatusBadRequest,
			respBody:       `{"message": "internal error"}`,
			expectErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tt.method, r.Method)
				assert.Equal(t, tt.path, r.URL.Path)

				data, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				defer assert.NoError(t, r.Body.Close())

				reqData := &structs.AddrRequest{}
				err = json.Unmarshal(data, reqData)
				require.NoError(t, err)

				assert.Equal(t, tt.body, reqData.Addr)

				w.WriteHeader(tt.respStatusCode)
				_, _ = fmt.Fprintln(w, tt.respBody)
			}))
			defer server.Close()

			// Get mocked server URL
			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)

			port, err := strconv.Atoi(serverURL.Port())
			require.NoError(t, err)

			p, err := NewPrysmClient(serverURL.Hostname(), port, 0, time.Second, nil)
			require.NoError(t, err)

			err = p.AddTrustedPeer(context.Background(), pid, maddr)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPrysmClient_RemoveTrustedPeer(t *testing.T) {
	otel.SetTracerProvider(tele.NoopTracerProvider())

	pid, err := peer.Decode("16Uiu2HAmBBTgCRezbBY8LbdfDN5PXYi3C1hwdoXJ9DZAorsWs4NR")
	require.NoError(t, err)

	tests := []struct {
		name           string
		method         string
		path           string
		respStatusCode int
		respBody       string
		expectErr      bool
	}{
		{
			name:           "success",
			method:         http.MethodDelete,
			path:           fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
			respStatusCode: http.StatusOK,
			respBody:       "",
			expectErr:      false,
		},
		{
			name:           "error_json_unmarshal",
			method:         http.MethodDelete,
			path:           fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
			respStatusCode: http.StatusBadRequest,
			respBody:       "{invalid_json}",
			expectErr:      true,
		},
		{
			name:           "invalid_response_status_code",
			method:         http.MethodDelete,
			path:           fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
			respStatusCode: http.StatusBadRequest,
			respBody:       `{"message": "internal error"}`,
			expectErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tt.method, r.Method)
				assert.Equal(t, tt.path, r.URL.Path)

				w.WriteHeader(tt.respStatusCode)
				_, _ = fmt.Fprintln(w, tt.respBody)
			}))
			defer server.Close()

			// Get mocked server URL.
			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)

			port, err := strconv.Atoi(serverURL.Port())
			require.NoError(t, err)

			p, err := NewPrysmClient(serverURL.Hostname(), port, 0, time.Second, nil)
			require.NoError(t, err)

			err = p.RemoveTrustedPeer(context.Background(), pid)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseHostAuth(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		wantHost    string
		wantUser    string
		wantPass    string
		wantErr     bool
		errContains string
	}{
		{
			name:     "no_auth",
			host:     "localhost",
			wantHost: "localhost",
		},
		{
			name:     "with_auth",
			host:     "user:pass@localhost",
			wantHost: "localhost",
			wantUser: "user",
			wantPass: "pass",
		},
		{
			name:     "with_special_chars_in_pass",
			host:     "user:p%40ss%3A123@localhost",
			wantHost: "localhost",
			wantUser: "user",
			wantPass: "p@ss:123",
		},
		{
			name:        "invalid_auth_format",
			host:        "user@localhost",
			wantErr:     true,
			errContains: "invalid auth format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth, err := parseHostAuth(tt.host)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, auth.Host)
			assert.Equal(t, tt.wantUser, auth.Username)
			assert.Equal(t, tt.wantPass, auth.Password)
		})
	}
}

func TestPrysmClient_AuthenticatedRequests(t *testing.T) {
	otel.SetTracerProvider(tele.NoopTracerProvider())

	maddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	require.NoError(t, err)

	pid, err := peer.Decode("16Uiu2HAmBBTgCRezbBY8LbdfDN5PXYi3C1hwdoXJ9DZAorsWs4NR")
	require.NoError(t, err)

	type request struct {
		method string
		path   string
		body   string // empty for GET/DELETE
	}

	type testCredentials struct {
		username string
		password string
	}

	tests := []struct {
		name           string
		host           string
		checkAuth      bool
		credentials    testCredentials
		respStatusCode int
		expectErr      bool
		requests       []request
	}{
		{
			name:           "no_auth_success",
			host:           "localhost",
			checkAuth:      false,
			respStatusCode: http.StatusOK,
			expectErr:      false,
			requests: []request{
				{
					method: http.MethodPost,
					path:   "/prysm/node/trusted_peers",
					body:   fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
				},
				{
					method: http.MethodGet,
					path:   "/prysm/node/trusted_peers",
				},
				{
					method: http.MethodDelete,
					path:   fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
				},
			},
		},
		{
			name:      "with_auth_success",
			host:      "testuser:testpass@localhost",
			checkAuth: true,
			credentials: testCredentials{
				username: "testuser",
				password: "testpass",
			},
			respStatusCode: http.StatusOK,
			expectErr:      false,
			requests: []request{
				{
					method: http.MethodPost,
					path:   "/prysm/node/trusted_peers",
					body:   fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
				},
				{
					method: http.MethodGet,
					path:   "/prysm/node/trusted_peers",
				},
				{
					method: http.MethodDelete,
					path:   fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
				},
			},
		},
		{
			name:      "with_auth_special_chars",
			host:      "testuser:test%40pass%3A123@localhost",
			checkAuth: true,
			credentials: testCredentials{
				username: "testuser",
				password: "test@pass:123",
			},
			respStatusCode: http.StatusOK,
			expectErr:      false,
			requests: []request{
				{
					method: http.MethodPost,
					path:   "/prysm/node/trusted_peers",
					body:   fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
				},
				{
					method: http.MethodGet,
					path:   "/prysm/node/trusted_peers",
				},
				{
					method: http.MethodDelete,
					path:   fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
				},
			},
		},
		{
			name:      "with_auth_unauthorized",
			host:      "testuser:wrongpass@localhost",
			checkAuth: true,
			credentials: testCredentials{
				username: "testuser",
				password: "testpass",
			},
			respStatusCode: http.StatusUnauthorized,
			expectErr:      true,
			requests: []request{
				{
					method: http.MethodPost,
					path:   "/prysm/node/trusted_peers",
					body:   fmt.Sprintf("%s/p2p/%s", maddr.String(), pid.String()),
				},
				{
					method: http.MethodGet,
					path:   "/prysm/node/trusted_peers",
				},
				{
					method: http.MethodDelete,
					path:   fmt.Sprintf("/prysm/node/trusted_peers/%s", pid.String()),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock HTTP server that checks auth.
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Check auth if required.
				if tt.checkAuth {
					user, pass, ok := r.BasicAuth()
					if !ok || user != tt.credentials.username || pass != tt.credentials.password {
						w.WriteHeader(http.StatusUnauthorized)

						return
					}
				}

				// Find matching request.
				var matchedReq *request
				for _, req := range tt.requests {
					if req.method == r.Method && req.path == r.URL.Path {
						matchedReq = &req

						break
					}
				}

				if matchedReq == nil {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)

					return
				}

				// Just return status code - we're only testing auth.
				w.WriteHeader(tt.respStatusCode)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("{}"))
			}))
			defer server.Close()

			// Get mocked server URL.
			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)

			port, err := strconv.Atoi(serverURL.Port())
			require.NoError(t, err)

			// Create client with the test host (which may include auth).
			p, err := NewPrysmClient(tt.host, port, 0, time.Second, nil)
			require.NoError(t, err)

			// Test AddTrustedPeer with auth.
			err = p.AddTrustedPeer(context.Background(), pid, maddr)
			if tt.expectErr {
				assert.Error(t, err)
				if tt.respStatusCode == http.StatusUnauthorized {
					assert.Contains(t, err.Error(), "authorization required")
				}
			} else {
				assert.NoError(t, err)
			}

			// Test ListTrustedPeers with auth.
			_, err = p.ListTrustedPeers(context.Background())
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Test RemoveTrustedPeer with auth.
			err = p.RemoveTrustedPeer(context.Background(), pid)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

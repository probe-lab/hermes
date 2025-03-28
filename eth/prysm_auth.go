package eth

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
)

// Add new struct to hold auth info
type AuthConfig struct {
	Username string
	Password string
	Host     string
}

// Add basic auth transport.
type basicAuthTransport struct {
	username string
	password string
	base     http.RoundTripper
}

// Add basic auth credentials for gRPC.
type basicAuthCreds struct {
	username string
	password string
}

// RoundTrip implements the RoundTripper interface.
func (t *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(t.username, t.password)

	return t.base.RoundTrip(req)
}

// GetRequestMetadata returns the request metadata for the basic auth credentials.
func (b *basicAuthCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	auth := fmt.Sprintf("%s:%s", b.username, b.password)

	return map[string]string{
		"authorization": fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth))),
	}, nil
}

// RequireTransportSecurity returns false because we're using HTTP.
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return false
}

func parseHostAuth(host string) (*AuthConfig, error) {
	// Parse the authority (user:pass@host) without scheme.
	u, err := url.Parse("//" + host)
	if err != nil {
		return nil, fmt.Errorf("invalid host format: %w", err)
	}

	auth := &AuthConfig{
		Host: u.Host,
	}

	// If we have user info, extract username and password.
	if u.User != nil {
		auth.Username = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			auth.Password = pass
		} else if auth.Username != "" {
			// If we have a username but no password, that's invalid.
			return nil, fmt.Errorf("invalid auth format: username provided without password")
		}
	}

	// If no host was parsed, use the original host string.
	if auth.Host == "" {
		auth.Host = host
	}

	return auth, nil
}

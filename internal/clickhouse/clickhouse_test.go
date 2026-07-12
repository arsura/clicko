package clickhouse

import (
	"context"
	"strings"
	"testing"
)

// TestDialParseErrorDoesNotLeakURI verifies that a URI failing to parse is
// never echoed back in the error. Every input here fails inside ParseDSN, so
// Dial returns before any network I/O. The passwords are deliberately the
// malformed shapes that make ParseDSN fail and embed the raw URI in its own
// error text (unencoded space, bad port, broken percent-escape).
func TestDialParseErrorDoesNotLeakURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "password with unencoded space",
			uri:  "clickhouse://user:Super Secret123@localhost:9000/db",
		},
		{
			name: "invalid port",
			uri:  "clickhouse://user:SuperSecret123@localhost:bad_port/db",
		},
		{
			name: "broken percent escape in password",
			uri:  "clickhouse://user:Super%zzSecret123@localhost:9000/db",
		},
		{
			name: "password with unencoded slash and question mark",
			uri:  "clickhouse://user:Super/Secret?123@localhost:9000/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, cleanup, err := Dial(context.Background(), tt.uri)
			if err == nil {
				t.Fatalf("Dial(%q) succeeded, want parse error", tt.uri)
			}
			if conn != nil || cleanup != nil {
				t.Errorf("Dial(%q) returned non-nil conn/cleanup alongside error", tt.uri)
			}
			if !strings.Contains(err.Error(), "failed to parse URI") {
				t.Errorf("Dial(%q) error %q, want it to mention \"failed to parse URI\"", tt.uri, err)
			}
			for _, secret := range []string{"Secret123", tt.uri} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("Dial(%q) error %q leaks %q", tt.uri, err, secret)
				}
			}
		})
	}
}

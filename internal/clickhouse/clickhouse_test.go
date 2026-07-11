package clickhouse

import "testing"

func TestRedactCredentials(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "password in dsn is redacted",
			in:   "clickhouse://admin:SuperSecret123@localhost:9000/db",
			want: "clickhouse://admin:xxxxx@localhost:9000/db",
		},
		{
			name: "password redacted inside a wrapping error message",
			in:   `parse "clickhouse://admin:SuperSecret123@localhost:bad_port/db": invalid port`,
			want: `parse "clickhouse://admin:xxxxx@localhost:bad_port/db": invalid port`,
		},
		{
			name: "empty password is left as-is",
			in:   "clickhouse://default:@localhost:9000/db",
			want: "clickhouse://default:@localhost:9000/db",
		},
		{
			name: "userinfo without password is left as-is",
			in:   "clickhouse://useronly@localhost:9000/db",
			want: "clickhouse://useronly@localhost:9000/db",
		},
		{
			name: "no userinfo is left as-is",
			in:   "clickhouse://localhost:9000/db",
			want: "clickhouse://localhost:9000/db",
		},
		{
			name: "password with special characters is redacted",
			in:   "clickhouse://user:p%40ss.w-rd@localhost:9000/db",
			want: "clickhouse://user:xxxxx@localhost:9000/db",
		},
		{
			name: "string without a dsn is unchanged",
			in:   "failed to connect: connection refused",
			want: "failed to connect: connection refused",
		},
		{
			name: "empty string is unchanged",
			in:   "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactCredentials(tt.in); got != tt.want {
				t.Errorf("redactCredentials(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

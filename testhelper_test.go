package ptest

import (
	"os"
	"testing"

	"github.com/portablesql/psql"

	// Register all backends so tests can use any engine via PSQL_TEST_DSN.
	_ "github.com/portablesql/psql-mysql"
	_ "github.com/portablesql/psql-pgsql"
	_ "github.com/portablesql/psql-sqlite"
)

// getTestBackend returns a backend for integration tests.
// It reads from PSQL_TEST_DSN env var, falling back to SQLite in-memory.
// Skips the test if no database is available.
func getTestBackend(t *testing.T) *psql.Backend {
	t.Helper()
	dsn := os.Getenv("PSQL_TEST_DSN")
	if dsn == "" {
		dsn = ":memory:"
	}
	be, err := psql.New(dsn)
	if err != nil {
		if os.Getenv("PSQL_TEST_DSN") != "" {
			// An explicitly requested database must be reachable, otherwise CI
			// would silently pass with every test skipped.
			t.Fatalf("PSQL_TEST_DSN is set but the database is not available: %s", err)
		}
		t.Skipf("database not available: %s", err)
	}
	// Every test opens its own pool; release it so a long run does not exhaust
	// the server's connection limit.
	t.Cleanup(func() { _ = be.Close() })
	return be
}

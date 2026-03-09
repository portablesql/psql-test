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
		t.Skipf("database not available: %s", err)
	}
	return be
}

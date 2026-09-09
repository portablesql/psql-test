package ptest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Named locks (psql.NamedLock / psql.WithNamedLock) need a dialect
// implementing psql.LockRenderer: GET_LOCK/RELEASE_LOCK on MySQL and
// MariaDB, pg_advisory_xact_lock on PostgreSQL. SQLite and CockroachDB have
// no named locks and must report ErrNotSupported.

func TestNamedLockSQLiteNotSupported(t *testing.T) {
	be := getTestBackend(t)
	if be.Engine() != psql.EngineSQLite {
		t.Skip("Test only applicable for SQLite")
	}
	ctx := be.Plug(context.Background())

	assert.False(t, be.Supports(psql.FeatureAdvisoryLocks))

	release, err := psql.NamedLock(ctx, "ptest-lock", time.Second)
	require.Error(t, err)
	assert.Nil(t, release)
	assert.True(t, errors.Is(err, psql.ErrNotSupported), err)

	called := false
	err = psql.WithNamedLock(ctx, "ptest-lock", time.Second, func(context.Context) error {
		called = true
		return nil
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	assert.False(t, called)
}

func TestNamedLockRoundTrip(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureAdvisoryLocks) {
		t.Skipf("named locks not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())

	release, err := psql.NamedLock(ctx, "ptest-lock", 5*time.Second)
	require.NoError(t, err, "the %s driver must implement LockRenderer", be.Variant())

	// a second holder must not get the lock while it is held
	_, err = psql.NamedLock(ctx, "ptest-lock", -1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, psql.ErrLockTimeout), err)

	require.NoError(t, release())

	// released: it can be taken again, inside a transaction this time
	var ran bool
	err = psql.WithNamedLock(ctx, "ptest-lock", time.Second, func(ctx context.Context) error {
		ran = true
		// re-entrant on the same connection (MySQL and PostgreSQL)
		inner, err := psql.NamedLock(ctx, "ptest-lock", -1)
		if err != nil {
			return err
		}
		return inner()
	})
	require.NoError(t, err)
	assert.True(t, ran)

	release, err = psql.NamedLock(ctx, "ptest-lock", -1)
	require.NoError(t, err)
	require.NoError(t, release())
}

package ptest

import (
	"context"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuilderLimitSyntax tests that LIMIT renders the same way on every engine:
// Limit(count) → LIMIT count, Limit(offset, count) → LIMIT count OFFSET offset.
func TestBuilderLimitSyntax(t *testing.T) {
	t.Run("Default LIMIT syntax", func(t *testing.T) {
		// Default context (no backend) should use default dialect
		ctx := context.Background()

		// Test LIMIT without offset
		query := psql.B().Select().From("users").Limit(10)
		sql, err := query.Render(ctx)
		require.NoError(t, err)
		assert.Equal(t, `SELECT * FROM "users" LIMIT 10`, sql)

		// Test LIMIT with offset: Limit(offset, count)
		query = psql.B().Select().From("users").Limit(10, 20)
		sql, err = query.Render(ctx)
		require.NoError(t, err)
		assert.Equal(t, `SELECT * FROM "users" LIMIT 20 OFFSET 10`, sql)
	})

	t.Run("Backend LIMIT syntax", func(t *testing.T) {
		be := getTestBackend(t)
		ctx := be.Plug(context.Background())

		// Test LIMIT without offset
		query := psql.B().Select().From("users").Limit(10)
		sql, err := query.Render(ctx)
		require.NoError(t, err)
		assert.Equal(t, `SELECT * FROM "users" LIMIT 10`, sql)

		// Test LIMIT with offset: identical on MySQL, PostgreSQL and SQLite
		query = psql.B().Select().From("users").Limit(10, 20)
		sql, err = query.Render(ctx)
		require.NoError(t, err)
		assert.Equal(t, `SELECT * FROM "users" LIMIT 20 OFFSET 10`, sql)
	})

	t.Run("LimitFrom matches Limit", func(t *testing.T) {
		// psql.LimitFrom(start, cnt) calls Limit(start, cnt): offset first
		ctx := context.Background()
		query := psql.B().Select().From("users").Limit(psql.LimitFrom(30, 5).LimitStart, psql.LimitFrom(30, 5).LimitCount)
		sql, err := query.Render(ctx)
		require.NoError(t, err)
		assert.Equal(t, `SELECT * FROM "users" LIMIT 5 OFFSET 30`, sql)
	})
}

// TestBuilderLimitBackwardCompatibility ensures the default (no backend)
// rendering matches the engine-specific one.
func TestBuilderLimitBackwardCompatibility(t *testing.T) {
	ctx := context.Background()

	query := psql.B().Select().From("products").Limit(50, 100)
	sql, err := query.Render(ctx)
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "products" LIMIT 100 OFFSET 50`, sql)
}

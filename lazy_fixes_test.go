package ptest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type LFItem struct {
	psql.Name `sql:"test_lf_item"`
	ID        int64  `sql:",key=PRIMARY"`
	Code      string `sql:",type=VARCHAR,size=32"`
	Label     string `sql:",type=VARCHAR,size=128"`
	Weight    float64
}

func setupLF(t *testing.T, labels ...string) context.Context {
	t.Helper()
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_lf_item"`).Exec(ctx)
	t.Cleanup(func() { _ = psql.Q(`DROP TABLE IF EXISTS "test_lf_item"`).Exec(ctx) })
	for i, l := range labels {
		require.NoError(t, psql.Insert(ctx, &LFItem{ID: int64(i + 1), Code: l, Label: l, Weight: float64(i) + 0.5}))
	}
	return ctx
}

func TestLazyFixesBatchNormalizesValues(t *testing.T) {
	ctx := setupLF(t, "one", "two", "three")

	// "01" and "1" both refer to ID 1; in batch mode both must resolve.
	f01 := psql.Lazy[LFItem]("ID", "01")
	f1 := psql.Lazy[LFItem]("ID", "1")
	f2 := psql.Lazy[LFItem]("ID", "2")
	fMissing := psql.Lazy[LFItem]("ID", "999")
	fBad := psql.Lazy[LFItem]("ID", "not-a-number")

	obj, err := f01.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "one", obj.Label)

	obj, err = f1.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "one", obj.Label)

	obj, err = f2.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "two", obj.Label)

	_, err = fMissing.Resolve(ctx)
	assert.True(t, errors.Is(err, os.ErrNotExist), "got %v", err)

	// A value that does not convert to the column type is left to the database.
	_, err = fBad.Resolve(ctx)
	assert.Error(t, err)

	// String columns batch too, and a column of a non-batchable Go type
	// (float64) still resolves, one query per value.
	fa := psql.Lazy[LFItem]("Code", "two")
	fb := psql.Lazy[LFItem]("Code", "three")
	obj, err = fa.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), obj.ID)
	obj, err = fb.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), obj.ID)

	fw := psql.Lazy[LFItem]("Weight", "1.5")
	obj, err = fw.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "two", obj.Label)
}

func TestLazyFixesContextBatch(t *testing.T) {
	ctx := setupLF(t, "one", "two", "three")
	bctx := psql.WithLazyBatch(ctx)

	f1 := psql.LazyCtx[LFItem](bctx, "ID", "1")
	f2 := psql.LazyCtx[LFItem](bctx, "ID", "2")
	f3 := psql.LazyCtx[LFItem](bctx, "ID", "3")
	other := psql.LazyCtx[LFItem](ctx, "ID", "3") // private batch, not shared

	// Resolving with a nil context uses the context given to LazyCtx.
	obj, err := f1.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "one", obj.Label)

	// Peers were resolved by the batch and are immediately available.
	obj, err = f2.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "two", obj.Label)
	obj, err = f3.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "three", obj.Label)

	// json.Marshal works without a global DefaultBackend.
	saved := psql.DefaultBackend
	psql.DefaultBackend = nil
	defer func() { psql.DefaultBackend = saved }()
	data, err := json.Marshal(map[string]any{"item": psql.LazyCtx[LFItem](bctx, "ID", "2")})
	require.NoError(t, err)
	assert.Contains(t, string(data), `"two"`)

	obj, err = other.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "three", obj.Label)
}

func TestLazyFixesCrossBackendIsolation(t *testing.T) {
	if os.Getenv("PSQL_TEST_DSN") != "" {
		t.Skip("needs two independent databases; only run with the in-memory SQLite default")
	}
	beA := getTestBackend(t)
	beB := getTestBackend(t)
	ctxA := beA.Plug(context.Background())
	ctxB := beB.Plug(context.Background())
	for _, ctx := range []context.Context{ctxA, ctxB} {
		_ = psql.Q(`DROP TABLE IF EXISTS "test_lf_item"`).Exec(ctx)
	}
	require.NoError(t, psql.Insert(ctxA, &LFItem{ID: 1, Code: "a", Label: "A-one"}))
	require.NoError(t, psql.Insert(ctxA, &LFItem{ID: 2, Code: "a", Label: "A-two"}))
	require.NoError(t, psql.Insert(ctxB, &LFItem{ID: 1, Code: "b", Label: "B-one"}))
	require.NoError(t, psql.Insert(ctxB, &LFItem{ID: 2, Code: "b", Label: "B-two"}))

	// Same batch, two backends: a leader only resolves peers bound to its backend.
	batch := psql.WithLazyBatch(context.Background())
	fA := psql.LazyCtx[LFItem](beA.Plug(batch), "ID", "1")
	fB := psql.LazyCtx[LFItem](beB.Plug(batch), "ID", "1")
	fA2 := psql.LazyCtx[LFItem](beA.Plug(batch), "ID", "2")
	assert.NotSame(t, fA, fB)

	obj, err := fA.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "A-one", obj.Label)
	obj, err = fA2.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "A-two", obj.Label)
	obj, err = fB.Resolve(nil)
	require.NoError(t, err)
	assert.Equal(t, "B-one", obj.Label)

	// Registry futures: a resolved future is dropped from the registry, so a
	// later Lazy call for the same key against another backend is fresh.
	f1 := psql.Lazy[LFItem]("ID", "2")
	obj, err = f1.Resolve(ctxA)
	require.NoError(t, err)
	assert.Equal(t, "A-two", obj.Label)
	f2 := psql.Lazy[LFItem]("ID", "2")
	assert.NotSame(t, f1, f2)
	obj, err = f2.Resolve(ctxB)
	require.NoError(t, err)
	assert.Equal(t, "B-two", obj.Label)

	// An unresolved registry future claimed by a leader on backend A must not
	// be handed to a leader on backend B: bind it first by resolving on A.
	pA := psql.Lazy[LFItem]("ID", "1")
	pB := psql.Lazy[LFItem]("Code", "b")
	obj, err = pA.Resolve(ctxA)
	require.NoError(t, err)
	assert.Equal(t, "A-one", obj.Label)
	obj, err = pB.Resolve(ctxB)
	require.NoError(t, err)
	assert.Equal(t, "b", obj.Code)

	for _, ctx := range []context.Context{ctxA, ctxB} {
		_ = psql.Q(`DROP TABLE IF EXISTS "test_lf_item"`).Exec(ctx)
	}
}

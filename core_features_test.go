package ptest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for the advanced core features (RETURNING, CTEs,
// multi-row inserts, JSON expressions, lock modes, transaction options, bulk
// inserts, EXPLAIN and feature detection). Every test runs on the engine
// selected by PSQL_TEST_DSN (SQLite in memory by default) and skips the parts
// the product does not support, as reported by Backend.Supports. Each test
// owns its cf_-prefixed table and drops it before and after running.

// cfDrop drops the named table, ignoring errors.
func cfDrop(ctx context.Context, name string) {
	_ = psql.Q(`DROP TABLE IF EXISTS "` + name + `"`).Exec(ctx)
}

// ---------------------------------------------------------------------------
// RETURNING
// ---------------------------------------------------------------------------

type CfReturningItem struct {
	psql.Name `sql:"cf_returning"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
	Hits      int64
}

func TestCoreFeaturesReturning(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureReturning) {
		t.Skipf("RETURNING not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_returning")
	defer cfDrop(ctx, "cf_returning")
	// create the table through the schema check
	require.NoError(t, psql.Insert(ctx, &CfReturningItem{ID: 1, Label: "seed", Hits: 5}))

	// INSERT ... RETURNING * scanned into objects by column name
	q := psql.B().Insert().Table("cf_returning").
		Set(map[string]any{"ID": 2, "Label": "inserted", "Hits": 7}).
		Returning("*")
	rows, err := psql.RunQueryT[CfReturningItem](ctx, q)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0].ID)
	assert.Equal(t, "inserted", rows[0].Label)
	assert.Equal(t, int64(7), rows[0].Hits)

	// the returned object is a regular tracked object: Update works on it
	rows[0].Hits = 8
	require.NoError(t, psql.Update(ctx, rows[0]))
	got, err := psql.Get[CfReturningItem](ctx, map[string]any{"ID": int64(2)})
	require.NoError(t, err)
	assert.Equal(t, int64(8), got.Hits)

	// RunQueryTOne with a subset of columns; unknown columns are ignored
	one, err := psql.RunQueryTOne[CfReturningItem](ctx, psql.B().Insert().Table("cf_returning").
		Set(map[string]any{"ID": 3, "Label": "third", "Hits": 0}).
		Returning("ID", psql.Raw(`"Hits"+1 AS "Hits"`)))
	require.NoError(t, err)
	assert.Equal(t, int64(3), one.ID)
	assert.Equal(t, int64(1), one.Hits, "expressions are returned as rendered")
	assert.Equal(t, "", one.Label, "columns not returned keep their zero value")

	// DELETE ... RETURNING (also available on MariaDB)
	deleted, err := psql.RunQueryT[CfReturningItem](ctx, psql.B().Delete().From("cf_returning").
		Where(map[string]any{"ID": int64(1)}).Returning("*"))
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	assert.Equal(t, "seed", deleted[0].Label)

	// UPDATE ... RETURNING is not available on MariaDB
	uq := psql.B().Update("cf_returning").Set(map[string]any{"Label": "renamed"}).
		Where(map[string]any{"ID": int64(2)}).Returning("ID", "Label")
	if be.Variant() == psql.VariantMariaDB {
		_, _, err = uq.RenderArgs(ctx)
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	} else {
		updated, err := psql.RunQueryT[CfReturningItem](ctx, uq)
		require.NoError(t, err)
		require.Len(t, updated, 1)
		assert.Equal(t, "renamed", updated[0].Label)
	}

	cnt, err := psql.Count[CfReturningItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

// ---------------------------------------------------------------------------
// Common table expressions
// ---------------------------------------------------------------------------

type CfCteItem struct {
	psql.Name `sql:"cf_cte"`
	ID        int64 `sql:",key=PRIMARY"`
	Parent    int64
	Label     string `sql:",type=VARCHAR,size=64"`
	Active    int64
}

func TestCoreFeaturesCTE(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureCTE) {
		t.Skipf("CTEs not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_cte")
	defer cfDrop(ctx, "cf_cte")
	require.NoError(t, psql.Insert(ctx,
		&CfCteItem{ID: 1, Parent: 0, Label: "root", Active: 1},
		&CfCteItem{ID: 2, Parent: 1, Label: "child", Active: 1},
		&CfCteItem{ID: 3, Parent: 2, Label: "grandchild", Active: 0},
		&CfCteItem{ID: 4, Parent: 0, Label: "other root", Active: 1},
	))

	// plain CTE used through a subquery
	active := psql.B().Select("ID").From("cf_cte").Where(map[string]any{"Active": 1})
	q := psql.B().With("active", active).
		Select("*").From("cf_cte").
		Where(map[string]any{"Parent": &psql.SubIn{Sub: psql.B().Select("ID").From("active")}}).
		OrderBy(psql.S("ID", "ASC"))
	rows, err := psql.RunQueryT[CfCteItem](ctx, q)
	require.NoError(t, err)
	require.Len(t, rows, 2, "children of active rows")
	assert.Equal(t, int64(2), rows[0].ID)
	assert.Equal(t, int64(3), rows[1].ID)

	// CTE joined with a column list
	counts := psql.B().Select("Parent", psql.Raw("COUNT(*)")).From("cf_cte").GroupByFields("Parent")
	jq := psql.B().With("pc", counts, "pid", "cnt").
		Select(psql.F("c", "ID"), psql.F("pc", "cnt")).From("cf_cte c").
		Join("INNER", "pc", psql.Equal(psql.F("pc.pid"), psql.F("c.ID"))).
		OrderBy(psql.S("c", "ID", "ASC"))
	res, err := jq.RunQuery(ctx)
	require.NoError(t, err)
	var ids, cnts []int64
	for res.Next() {
		var id, cnt int64
		require.NoError(t, res.Scan(&id, &cnt))
		ids = append(ids, id)
		cnts = append(cnts, cnt)
	}
	require.NoError(t, res.Err())
	res.Close()
	assert.Equal(t, []int64{1, 2}, ids)
	assert.Equal(t, []int64{1, 1}, cnts)

	// recursive CTE written with Raw (the builder has no UNION)
	tree := psql.Raw(`SELECT "ID","Parent" FROM "cf_cte" WHERE "ID"=1 UNION ALL SELECT n."ID",n."Parent" FROM "cf_cte" n JOIN "tree" t ON n."Parent"=t."ID"`)
	rq := psql.B().WithRecursive("tree", tree, "ID", "Parent").Select("ID").From("tree").OrderBy(psql.S("ID", "ASC"))
	res, err = rq.RunQuery(ctx)
	require.NoError(t, err)
	ids = nil
	for res.Next() {
		var id int64
		require.NoError(t, res.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, res.Err())
	res.Close()
	assert.Equal(t, []int64{1, 2, 3}, ids, "the subtree of node 1")
}

// ---------------------------------------------------------------------------
// Multi-row inserts
// ---------------------------------------------------------------------------

type CfRowsItem struct {
	psql.Name `sql:"cf_rows"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
	Hits      int64
}

func TestCoreFeaturesInsertRows(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_rows")
	defer cfDrop(ctx, "cf_rows")
	require.NoError(t, be.CheckStructure(ctx, psql.Table[CfRowsItem]()))

	// column-list form
	q := psql.B().InsertRows([]string{"ID", "Label", "Hits"},
		[]any{1, "a", 1},
		[]any{2, "b", 2},
		[]any{3, "c", 3},
	).Table("cf_rows")
	res, err := q.ExecQuery(ctx)
	require.NoError(t, err)
	if n, err := res.RowsAffected(); err == nil {
		assert.Equal(t, int64(3), n)
	}

	// map form: columns are the sorted keys, every row needs the same keys
	_, err = psql.B().Values(
		map[string]any{"ID": 4, "Label": "d", "Hits": psql.Raw("4*10")},
		map[string]any{"ID": 5, "Label": "e", "Hits": 5},
	).Table("cf_rows").ExecQuery(ctx)
	require.NoError(t, err)

	cnt, err := psql.Count[CfRowsItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 5, cnt)
	d, err := psql.Get[CfRowsItem](ctx, map[string]any{"ID": int64(4)})
	require.NoError(t, err)
	assert.Equal(t, int64(40), d.Hits, "expressions are expanded")

	// conflict clauses apply to every row: upsert with Excluded
	_, err = psql.B().InsertRows([]string{"ID", "Label", "Hits"},
		[]any{1, "a2", 10},
		[]any{6, "f", 6},
	).Table("cf_rows").
		OnConflict("ID").
		DoUpdate(map[string]any{"Label": psql.Excluded("Label"), "Hits": psql.Excluded("Hits")}).
		ExecQuery(ctx)
	require.NoError(t, err)
	a, err := psql.Get[CfRowsItem](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "a2", a.Label)
	assert.Equal(t, int64(10), a.Hits)
	cnt, err = psql.Count[CfRowsItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 6, cnt)

	// DoNothing skips the conflicting row
	_, err = psql.B().InsertRows([]string{"ID", "Label", "Hits"},
		[]any{1, "ignored", 0},
		[]any{7, "g", 7},
	).Table("cf_rows").DoNothing().ExecQuery(ctx)
	require.NoError(t, err)
	a, err = psql.Get[CfRowsItem](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "a2", a.Label)
	cnt, err = psql.Count[CfRowsItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 7, cnt)
}

// ---------------------------------------------------------------------------
// JSON expressions
// ---------------------------------------------------------------------------

type CfJSONItem struct {
	psql.Name `sql:"cf_json"`
	ID        int64          `sql:",key=PRIMARY"`
	Data      map[string]any `sql:",import=JSON,format=json"`
}

func TestCoreFeaturesJSON(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureJSON) {
		t.Skipf("JSON expressions not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_json")
	defer cfDrop(ctx, "cf_json")
	require.NoError(t, psql.Insert(ctx,
		&CfJSONItem{ID: 1, Data: map[string]any{"name": "alice", "role": "admin", "prefs": map[string]any{"theme": "light"}, "rank": 2}},
		&CfJSONItem{ID: 2, Data: map[string]any{"name": "bob", "role": "user", "prefs": map[string]any{"theme": "light"}, "rank": 1}},
		&CfJSONItem{ID: 3, Data: map[string]any{"name": "carol", "rank": 3}},
	))

	// JSONGetText in a WHERE comparison
	rows, err := psql.RunQueryT[CfJSONItem](ctx, psql.B().Select().From("cf_json").
		Where(psql.Equal(psql.JSONGetText("Data", "name"), "bob")))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0].ID)

	// nested path, and ORDER BY on a JSON value
	rows, err = psql.RunQueryT[CfJSONItem](ctx, psql.B().Select().From("cf_json").
		Where(psql.Equal(psql.JSONGetText("Data", "prefs", "theme"), "light")).
		OrderBy(psql.JSONGetText("Data", "rank")))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(2), rows[0].ID, "rank 1 first")
	assert.Equal(t, int64(1), rows[1].ID)

	// JSONGet / JSONGetText selected as columns
	res, err := psql.B().Select(psql.JSONGetText("Data", "name")).From("cf_json").
		Where(map[string]any{"ID": int64(1)}).RunQuery(ctx)
	require.NoError(t, err)
	require.True(t, res.Next())
	var name string
	require.NoError(t, res.Scan(&name))
	res.Close()
	assert.Equal(t, "alice", name)

	// JSONHasKey, as an expression and as a WHERE map value
	rows, err = psql.RunQueryT[CfJSONItem](ctx, psql.B().Select().From("cf_json").
		Where(psql.JSONHasKey("Data", "role")).OrderBy(psql.S("ID", "ASC")))
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	rows, err = psql.RunQueryT[CfJSONItem](ctx, psql.B().Select().From("cf_json").
		Where(map[string]any{"Data": &psql.Not{V: psql.JSONHasKey(nil, "role")}}))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(3), rows[0].ID)

	// JSONSet as an UPDATE value: replaces a nested value, creates a key
	_, err = psql.B().Update("cf_json").Set(map[string]any{
		"Data": psql.JSONSet("Data", []string{"prefs", "theme"}, "\"dark\""),
	}).Where(map[string]any{"ID": int64(1)}).ExecQuery(ctx)
	require.NoError(t, err)
	_, err = psql.B().Update("cf_json").Set(map[string]any{
		"Data": psql.JSONSet("Data", []string{"score"}, 42),
	}).Where(map[string]any{"ID": int64(1)}).ExecQuery(ctx)
	require.NoError(t, err)
	got, err := psql.Get[CfJSONItem](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "dark", got.Data["prefs"].(map[string]any)["theme"])
	assert.Equal(t, float64(42), got.Data["score"])
	assert.Equal(t, "alice", got.Data["name"], "other keys are kept")

	// JSONContains: PostgreSQL @> and MySQL JSON_CONTAINS; SQLite has no
	// containment operator and fails to render
	cq := psql.B().Select().From("cf_json").Where(psql.JSONContains("Data", map[string]any{"role": "admin"}))
	if be.Engine() == psql.EngineSQLite {
		_, _, err := cq.RenderArgs(ctx)
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	} else {
		rows, err = psql.RunQueryT[CfJSONItem](ctx, cq)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(1), rows[0].ID)
		// WHERE map form
		rows, err = psql.RunQueryT[CfJSONItem](ctx, psql.B().Select().From("cf_json").
			Where(map[string]any{"Data": psql.JSONContains(nil, map[string]any{"prefs": map[string]any{"theme": "light"}})}))
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(2), rows[0].ID)
	}
}

// ---------------------------------------------------------------------------
// Row lock modes
// ---------------------------------------------------------------------------

type CfLockItem struct {
	psql.Name `sql:"cf_lock"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreFeaturesLockModes(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_lock")
	defer cfDrop(ctx, "cf_lock")
	require.NoError(t, psql.Insert(ctx, &CfLockItem{ID: 1, Label: "a"}, &CfLockItem{ID: 2, Label: "b"}))

	// rendering per product
	sqlText, err := psql.B().Select().From("cf_lock").SetLockMode(psql.LockShare).Render(ctx)
	require.NoError(t, err)
	switch be.Variant() {
	case psql.VariantSQLite:
		assert.False(t, strings.Contains(sqlText, "FOR "), sqlText)
		assert.False(t, strings.Contains(sqlText, "LOCK"), sqlText)
	case psql.VariantMariaDB:
		assert.True(t, strings.HasSuffix(sqlText, " LOCK IN SHARE MODE"), sqlText)
	default:
		assert.True(t, strings.HasSuffix(sqlText, " FOR SHARE") || strings.HasSuffix(sqlText, " LOCK IN SHARE MODE"), sqlText)
	}
	sqlText, err = psql.B().Select().From("cf_lock").SetLockMode(psql.LockKeyShare).SetSkipLocked().Render(ctx)
	require.NoError(t, err)
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		assert.True(t, strings.HasSuffix(sqlText, " FOR KEY SHARE SKIP LOCKED"), sqlText)
	case psql.EngineSQLite:
		assert.False(t, strings.Contains(sqlText, "SKIP LOCKED"), sqlText)
	}

	// executed inside a transaction: FetchLockShare, WithLock and the
	// legacy FetchLock
	err = psql.Tx(ctx, func(txCtx context.Context) error {
		rows, err := psql.Fetch[CfLockItem](txCtx, nil, psql.FetchLockShare, psql.Sort(psql.S("ID", "ASC")))
		if err != nil {
			return err
		}
		if len(rows) != 2 {
			return fmt.Errorf("expected 2 rows, got %d", len(rows))
		}
		one, err := psql.Get[CfLockItem](txCtx, map[string]any{"ID": int64(1)}, psql.WithLock(psql.LockUpdate))
		if err != nil {
			return err
		}
		if one.Label != "a" {
			return fmt.Errorf("unexpected label %q", one.Label)
		}
		_, err = psql.Fetch[CfLockItem](txCtx, map[string]any{"ID": int64(2)}, psql.FetchLock, psql.FetchLockNoWait)
		if err != nil {
			return err
		}
		if be.Engine() == psql.EnginePostgreSQL {
			// PostgreSQL-only modes; FOR ... OF restricts the lock to a table
			_, err = psql.Fetch[CfLockItem](txCtx, nil, psql.FetchLockNoKeyUpdate)
			if err != nil {
				return err
			}
			_, err = psql.Fetch[CfLockItem](txCtx, nil, psql.WithLock(psql.LockKeyShare, "cf_lock"))
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Transaction options
// ---------------------------------------------------------------------------

type CfTxItem struct {
	psql.Name `sql:"cf_tx"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreFeaturesTxWithOptions(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_tx")
	defer cfDrop(ctx, "cf_tx")
	require.NoError(t, psql.Insert(ctx, &CfTxItem{ID: 1, Label: "seed"}))

	// serializable transaction committing an insert
	calls := 0
	err := psql.TxWithOptions(ctx, &psql.TxOptions{Isolation: sql.LevelSerializable}, func(txCtx context.Context) error {
		calls++
		return psql.Insert(txCtx, &CfTxItem{ID: 2, Label: "serializable"})
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)

	// read-only transaction reading the committed rows
	err = psql.TxWithOptions(ctx, &psql.TxOptions{ReadOnly: true}, func(txCtx context.Context) error {
		cnt, err := psql.Count[CfTxItem](txCtx, nil)
		if err != nil {
			return err
		}
		if cnt != 2 {
			return fmt.Errorf("expected 2 rows, got %d", cnt)
		}
		return nil
	})
	require.NoError(t, err)

	// retries disabled: a plain error is returned as is, after one attempt
	calls = 0
	boom := errors.New("boom")
	err = psql.TxWithOptions(ctx, &psql.TxOptions{MaxRetries: -1}, func(txCtx context.Context) error {
		calls++
		if err := psql.Insert(txCtx, &CfTxItem{ID: 3, Label: "rolled back"}); err != nil {
			return err
		}
		return boom
	})
	assert.Equal(t, boom, err)
	assert.Equal(t, 1, calls)
	assert.False(t, errors.Is(err, psql.ErrTxRetriesExhausted))
	cnt, err := psql.Count[CfTxItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)

	// a custom backoff is only consulted when a retry happens
	backoffCalls := 0
	err = psql.TxWithOptions(ctx, &psql.TxOptions{
		MaxRetries: 5,
		Backoff:    func(int) time.Duration { backoffCalls++; return 0 },
	}, func(txCtx context.Context) error {
		return psql.Insert(txCtx, &CfTxItem{ID: 4, Label: "no retry needed"})
	})
	require.NoError(t, err)
	assert.Equal(t, 0, backoffCalls)

	// a duplicate key is never retryable
	err = psql.Tx(ctx, func(txCtx context.Context) error {
		return psql.Insert(txCtx, &CfTxItem{ID: 1, Label: "dup"})
	})
	require.Error(t, err)
	assert.True(t, psql.IsDuplicate(err))
	assert.False(t, psql.IsRetryable(err))
}

// ---------------------------------------------------------------------------
// Bulk insert
// ---------------------------------------------------------------------------

type CfBulkItem struct {
	psql.Name `sql:"cf_bulk"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
	Rank      int64
}

func TestCoreFeaturesBulkInsert(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_bulk")
	defer cfDrop(ctx, "cf_bulk")

	const n = 1500
	rows := make([]*CfBulkItem, n)
	for i := range rows {
		rows[i] = &CfBulkItem{ID: int64(i + 1), Label: fmt.Sprintf("row-%04d", i), Rank: int64(i % 7)}
	}
	require.NoError(t, psql.BulkInsert(ctx, rows, psql.BulkBatchSize(400)))

	cnt, err := psql.Count[CfBulkItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, n, cnt)

	// content and order are preserved across batches
	last, err := psql.Get[CfBulkItem](ctx, map[string]any{"ID": int64(n)})
	require.NoError(t, err)
	assert.Equal(t, "row-1499", last.Label)
	mid, err := psql.Get[CfBulkItem](ctx, map[string]any{"ID": int64(401)})
	require.NoError(t, err)
	assert.Equal(t, "row-0400", mid.Label)
	assert.Equal(t, int64(400%7), mid.Rank)

	// BulkIgnore skips the conflicting rows and keeps the existing ones
	more := []*CfBulkItem{{ID: 1, Label: "dup"}, {ID: n + 1, Label: "new"}}
	require.NoError(t, psql.BulkInsert(ctx, more, psql.BulkIgnore(), psql.BulkNoHooks()))
	cnt, err = psql.Count[CfBulkItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, n+1, cnt)
	first, err := psql.Get[CfBulkItem](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "row-0000", first.Label)

	// an empty slice is a no-op
	require.NoError(t, psql.BulkInsert[CfBulkItem](ctx, nil))
}

// ---------------------------------------------------------------------------
// EXPLAIN
// ---------------------------------------------------------------------------

type CfExplainItem struct {
	psql.Name `sql:"cf_explain"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreFeaturesExplain(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	cfDrop(ctx, "cf_explain")
	defer cfDrop(ctx, "cf_explain")
	require.NoError(t, psql.Insert(ctx, &CfExplainItem{ID: 1, Label: "a"}, &CfExplainItem{ID: 2, Label: "b"}))

	q := psql.B().Select().From("cf_explain").Where(map[string]any{"ID": int64(1)})
	plan, err := q.Explain(ctx, false)
	require.NoError(t, err)
	assert.NotEmpty(t, strings.TrimSpace(plan), "the plan must not be empty")
	t.Logf("plan on %s:\n%s", be.Variant(), plan)

	// EXPLAIN ANALYZE executes the query; SELECT is harmless
	plan, err = q.Explain(ctx, true)
	if errors.Is(err, psql.ErrNotSupported) {
		t.Logf("EXPLAIN ANALYZE not available on %s %s", be.Variant(), be.ServerVersion())
	} else {
		require.NoError(t, err)
		assert.NotEmpty(t, strings.TrimSpace(plan))
	}

	// rendering errors are reported before anything is sent
	_, err = psql.B().Select().From("cf_explain").Where("bare string").Explain(ctx, false)
	require.Error(t, err)

	// the rows are untouched
	cnt, err := psql.Count[CfExplainItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

// ---------------------------------------------------------------------------
// Feature detection
// ---------------------------------------------------------------------------

func TestCoreFeaturesSupportsVariant(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	v := be.Variant()
	t.Logf("engine %s, variant %s, server version %q", be.Engine(), v, be.ServerVersion())

	// the variant belongs to the backend's engine and is never unknown
	assert.NotEqual(t, psql.VariantUnknown, v)
	assert.Equal(t, be.Engine(), v.Engine())
	assert.NotEqual(t, "Unknown", v.String())

	// a nil backend supports nothing, and unknown features are unsupported
	var none *psql.Backend
	assert.False(t, none.Supports(psql.FeatureCTE))
	assert.Equal(t, psql.VariantUnknown, none.Variant())
	assert.False(t, be.Supports("no-such-feature"))

	// engine-level invariants: whatever the dialect answers, these hold
	assert.True(t, be.Supports(psql.FeatureCTE), "every supported product has CTEs")
	assert.True(t, be.Supports(psql.FeatureJSON))
	assert.True(t, be.Supports(psql.FeatureIdentityColumns))
	switch v {
	case psql.VariantMySQL:
		assert.False(t, be.Supports(psql.FeatureReturning))
	case psql.VariantPostgreSQL, psql.VariantCockroachDB, psql.VariantSQLite, psql.VariantMariaDB:
		assert.True(t, be.Supports(psql.FeatureReturning))
	}
	if be.Supports(psql.FeatureAsOfSystemTime) || be.Supports(psql.FeatureRowTTL) {
		assert.Equal(t, psql.VariantCockroachDB, v)
	}
	if be.Supports(psql.FeatureListenNotify) {
		assert.Equal(t, psql.VariantPostgreSQL, v)
	}
	if v == psql.VariantSQLite || v == psql.VariantCockroachDB {
		assert.False(t, be.Supports(psql.FeatureAdvisoryLocks))
	}
	assert.Equal(t, be.Engine() == psql.EnginePostgreSQL, be.Supports(psql.FeatureDistinctOn))
	if be.Supports(psql.FeatureBulkCopy) || be.Supports(psql.FeatureVectors) {
		assert.Equal(t, psql.EnginePostgreSQL, be.Engine())
	}
	if v == psql.VariantSQLite {
		assert.False(t, be.Supports(psql.FeatureFullText))
	}

	// rendering agrees with Supports: features either render or fail with
	// ErrNotSupported, never anything else
	_, err := psql.B().Select("*").DistinctOn("a").From("t").OrderBy(psql.S("a")).Render(ctx)
	if be.Supports(psql.FeatureDistinctOn) {
		assert.NoError(t, err)
	} else {
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	}
	_, err = psql.B().Select().From("t").AsOfSystemTime("-10s").Render(ctx)
	if be.Supports(psql.FeatureAsOfSystemTime) {
		assert.NoError(t, err)
	} else {
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	}
	_, err = psql.B().Update("t").Set(map[string]any{"a": 1}).Returning("a").Render(ctx)
	if !be.Supports(psql.FeatureReturning) {
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	}
	_, err = psql.B().Select().From("t").Where(psql.FullText("x", "a")).Render(ctx)
	if be.Supports(psql.FeatureFullText) {
		assert.NoError(t, err)
	} else {
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	}
	if !be.Supports(psql.FeatureAdvisoryLocks) {
		_, err = psql.NamedLock(ctx, "cf-lock", -1)
		assert.True(t, errors.Is(err, psql.ErrNotSupported), err)
	}
}

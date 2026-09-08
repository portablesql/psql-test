package ptest

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for the core fixes (change tracking, options, transactions,
// savepoints, schema check, LastInsertId). They run on the engine selected by
// PSQL_TEST_DSN, SQLite in memory by default.

type CoreSortItem struct {
	psql.Name `sql:"core_sort_item"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreGetSort(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_sort_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_sort_item"`).Exec(ctx) }()

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, psql.Insert(ctx, &CoreSortItem{ID: i, Label: "l"}))
	}

	last, err := psql.Get[CoreSortItem](ctx, nil, psql.Sort(psql.S("ID", "DESC")))
	require.NoError(t, err)
	assert.Equal(t, int64(3), last.ID, "Get must honor Sort")

	first, err := psql.Get[CoreSortItem](ctx, nil, psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.ID)

	// FetchMapped / FetchGrouped: key validation and options
	_, err = psql.FetchMapped[CoreSortItem](ctx, nil, "Nope")
	assert.ErrorIs(t, err, psql.ErrUnknownField)
	m, err := psql.FetchMapped[CoreSortItem](ctx, nil, "ID", psql.Sort(psql.S("ID", "DESC")), psql.Limit(2))
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Contains(t, m, "3")
	assert.Contains(t, m, "2")
	g, err := psql.FetchGrouped[CoreSortItem](ctx, nil, "Label")
	require.NoError(t, err)
	assert.Len(t, g["l"], 3)
}

type CoreNullItem struct {
	psql.Name `sql:"core_null_item"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
	Score     *int64
	DeletedAt *time.Time
}

func TestCoreHasChangedNullable(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_null_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_null_item"`).Exec(ctx) }()

	seven := int64(7)
	require.NoError(t, psql.Insert(ctx, &CoreNullItem{ID: 1, Label: "one"}))
	require.NoError(t, psql.Insert(ctx, &CoreNullItem{ID: 2, Label: "two", Score: &seven}))

	one, err := psql.Get[CoreNullItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	assert.Nil(t, one.Score)
	assert.Nil(t, one.DeletedAt)
	assert.False(t, psql.HasChanged(one), "freshly loaded row with NULL columns is not changed")

	two, err := psql.Get[CoreNullItem](ctx, map[string]any{"ID": 2})
	require.NoError(t, err)
	require.NotNil(t, two.Score)
	assert.False(t, psql.HasChanged(two))
	*two.Score = 8
	assert.True(t, psql.HasChanged(two))

	// Update writes only the changed column: a concurrent change to another
	// column must survive the Update.
	one.Label = "uno"
	assert.True(t, psql.HasChanged(one))
	_, err = psql.B().Update("core_null_item").Set(map[string]any{"Score": 99}).Where(map[string]any{"ID": 1}).ExecQuery(ctx)
	require.NoError(t, err)
	require.NoError(t, psql.Update(ctx, one))
	assert.False(t, psql.HasChanged(one))

	check, err := psql.Get[CoreNullItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	assert.Equal(t, "uno", check.Label)
	require.NotNil(t, check.Score, "Update must not have rewritten the untouched NULL column")
	assert.Equal(t, int64(99), *check.Score)

	// a NULL column set to a value is written
	now := time.Now().UTC().Truncate(time.Second)
	one.DeletedAt = &now
	require.NoError(t, psql.Update(ctx, one))
	cnt, err := psql.Count[CoreNullItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	assert.Equal(t, 0, cnt, "row is now soft deleted")
	cnt, err = psql.Count[CoreNullItem](ctx, map[string]any{"ID": 1}, psql.IncludeDeleted())
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	// Restore(nil) restores everything
	_, err = psql.Restore[CoreNullItem](ctx, nil)
	require.NoError(t, err)
	cnt, err = psql.Count[CoreNullItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

type CoreTxItem struct {
	psql.Name `sql:"core_tx_item"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreQExecInsideTx(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx) }()
	require.NoError(t, psql.Insert(ctx, &CoreTxItem{ID: 1, Label: "seed"}))

	// Q().Exec must run on the transaction: rolled back here
	done := make(chan error, 1)
	go func() {
		done <- psql.Tx(ctx, func(txCtx context.Context) error {
			if err := psql.Q(`INSERT INTO "core_tx_item" ("ID","Label") VALUES (?,?)`, 2, "in tx").Exec(txCtx); err != nil {
				return err
			}
			return errors.New("abort")
		})
	}()
	select {
	case err := <-done:
		assert.EqualError(t, err, "abort")
	case <-time.After(10 * time.Second):
		t.Fatal("Q().Exec inside a transaction deadlocked (ran outside the transaction)")
	}
	cnt, err := psql.Count[CoreTxItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "statement executed with Q().Exec must be part of the rolled back transaction")

	// committed
	err = psql.Tx(ctx, func(txCtx context.Context) error {
		return psql.Q(`INSERT INTO "core_tx_item" ("ID","Label") VALUES (?,?)`, 3, "in tx").Exec(txCtx)
	})
	require.NoError(t, err)
	cnt, err = psql.Count[CoreTxItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

func TestCoreSavepoints(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx) }()

	err := psql.Tx(ctx, func(txCtx context.Context) error {
		if err := psql.Insert(txCtx, &CoreTxItem{ID: 1, Label: "outer"}); err != nil {
			return err
		}
		// inner rolled back to savepoint
		inner := psql.Tx(txCtx, func(innerCtx context.Context) error {
			if err := psql.Insert(innerCtx, &CoreTxItem{ID: 2, Label: "inner-rollback"}); err != nil {
				return err
			}
			return errors.New("undo")
		})
		if inner == nil || inner.Error() != "undo" {
			return errors.New("unexpected inner result")
		}
		// inner released
		if err := psql.Tx(txCtx, func(innerCtx context.Context) error {
			return psql.Insert(innerCtx, &CoreTxItem{ID: 3, Label: "inner-commit"})
		}); err != nil {
			return err
		}
		// two levels deep
		return psql.Tx(txCtx, func(l1 context.Context) error {
			return psql.Tx(l1, func(l2 context.Context) error {
				return psql.Insert(l2, &CoreTxItem{ID: 4, Label: "deep"})
			})
		})
	})
	require.NoError(t, err)

	rows, err := psql.Fetch[CoreTxItem](ctx, nil, psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	var ids []int64
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	assert.Equal(t, []int64{1, 3, 4}, ids)

	// explicit proxies: double commit, rollback after commit
	tx, err := psql.BeginTx(ctx, nil)
	require.NoError(t, err)
	inner, err := tx.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, psql.Insert(psql.ContextTx(ctx, inner), &CoreTxItem{ID: 5, Label: "x"}))
	// outer cannot finish while the inner one is open
	err = tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still open")
	require.NoError(t, inner.Rollback())
	assert.ErrorIs(t, inner.Commit(), psql.ErrTxAlreadyProcessed)
	require.NoError(t, tx.Commit(), "outer proxy is still usable after the refused Commit")
	assert.ErrorIs(t, tx.Rollback(), psql.ErrTxAlreadyProcessed)
	cnt, err := psql.Count[CoreTxItem](ctx, map[string]any{"ID": 5})
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)
}

func TestCoreEscapeTxIntegration(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_tx_item"`).Exec(ctx) }()
	require.NoError(t, psql.Insert(ctx, &CoreTxItem{ID: 1, Label: "seed"}))

	err := psql.Tx(ctx, func(txCtx context.Context) error {
		outer, ok := psql.EscapeTx(txCtx)
		if !ok {
			return errors.New("EscapeTx did not find the transaction")
		}
		if psql.GetBackend(outer) != be {
			return errors.New("escaped context lost the backend")
		}
		if _, again := psql.EscapeTx(outer); again {
			return errors.New("escaped context still carries a transaction")
		}
		if be.Engine() == psql.EngineSQLite {
			// SQLite backends use a single connection: a statement outside
			// the transaction would wait for it, so only the escape itself
			// is verified here.
			return errors.New("abort")
		}
		if err := psql.Insert(outer, &CoreTxItem{ID: 2, Label: "audit"}); err != nil {
			return err
		}
		return errors.New("abort")
	})
	assert.EqualError(t, err, "abort")

	if be.Engine() != psql.EngineSQLite {
		cnt, err := psql.Count[CoreTxItem](ctx, map[string]any{"ID": 2})
		require.NoError(t, err)
		assert.Equal(t, 1, cnt, "row written through EscapeTx must survive the rollback")
	}
}

type CoreAutoItem struct {
	psql.Name `sql:"core_auto_item"`
	ID        *int64 `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestCoreLastInsertId(t *testing.T) {
	be := getTestBackend(t)
	if be.Engine() == psql.EnginePostgreSQL {
		t.Skip("PostgreSQL uses RETURNING instead of LastInsertId")
	}
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_auto_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_auto_item"`).Exec(ctx) }()

	if be.Engine() == psql.EngineMySQL {
		// create with AUTO_INCREMENT so the server generates ids
		require.NoError(t, psql.Q("CREATE TABLE `core_auto_item` (`ID` BIGINT NOT NULL AUTO_INCREMENT, `Label` VARCHAR(64), PRIMARY KEY (`ID`))").Exec(ctx))
	}

	a := &CoreAutoItem{Label: "a"}
	require.NoError(t, psql.Insert(ctx, a))
	require.NotNil(t, a.ID, "primary key must be populated from LastInsertId")
	b := &CoreAutoItem{Label: "b"}
	require.NoError(t, psql.Insert(ctx, b))
	require.NotNil(t, b.ID)
	assert.NotEqual(t, *a.ID, *b.ID)

	got, err := psql.Get[CoreAutoItem](ctx, map[string]any{"ID": *b.ID})
	require.NoError(t, err)
	assert.Equal(t, "b", got.Label)
}

type CoreSoftAttrItem struct {
	psql.Name `sql:"core_soft_attr_item"`
	ID        int64      `sql:",key=PRIMARY"`
	Label     string     `sql:",type=VARCHAR,size=64"`
	Removed   *time.Time `sql:",softdelete"`
}

func TestCoreSoftDeleteAttribute(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_soft_attr_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_soft_attr_item"`).Exec(ctx) }()

	// the table is created with the softdelete column (type inferred)
	require.NoError(t, psql.Insert(ctx, &CoreSoftAttrItem{ID: 1, Label: "a"}))
	require.NoError(t, psql.Insert(ctx, &CoreSoftAttrItem{ID: 2, Label: "b"}))

	_, err := psql.Delete[CoreSoftAttrItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	cnt, err := psql.Count[CoreSoftAttrItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	all, err := psql.Fetch[CoreSoftAttrItem](ctx, nil, psql.IncludeDeleted(), psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.NotNil(t, all[0].Removed)
	assert.Nil(t, all[1].Removed)
	assert.False(t, psql.HasChanged(all[0]))

	_, err = psql.Restore[CoreSoftAttrItem](ctx, nil)
	require.NoError(t, err)
	cnt, err = psql.Count[CoreSoftAttrItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
}

type CoreConcurrentItem struct {
	psql.Name `sql:"core_concurrent_item"`
	ID        int64 `sql:",key=PRIMARY"`
	Stamp     time.Time
}

func TestCoreConcurrentFirstUse(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_concurrent_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_concurrent_item"`).Exec(ctx) }()

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := psql.Insert(ctx, &CoreConcurrentItem{ID: int64(i + 1), Stamp: time.Now()}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent insert failed: %s", err)
	}
	cnt, err := psql.Count[CoreConcurrentItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 16, cnt)
}

type CoreNoAutoItem struct {
	psql.Name `sql:"core_noauto_item"`
	ID        int64 `sql:",key=PRIMARY"`
}

func TestCoreSchemaCheckDisabled(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "core_noauto_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "core_noauto_item"`).Exec(ctx) }()

	noAuto := psql.NewBackend(be.Engine(), be.DB(), psql.WithSchemaCheck(false), psql.WithDriverData(be.DriverData()))
	nctx := noAuto.Plug(context.Background())

	// no automatic DDL: the table is missing, the insert fails
	err := psql.Insert(nctx, &CoreNoAutoItem{ID: 1})
	require.Error(t, err, "table must not have been created implicitly")
	var perr *psql.Error
	assert.ErrorAs(t, err, &perr)

	// explicit check creates it
	require.NoError(t, noAuto.CheckStructure(nctx, psql.Table[CoreNoAutoItem]()))
	require.NoError(t, psql.Insert(nctx, &CoreNoAutoItem{ID: 1}))
	cnt, err := psql.Count[CoreNoAutoItem](nctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

type CoreNamedProfile struct {
	psql.Key  `sql:"PRIMARY,type=PRIMARY,fields='ProfileId'"`
	ProfileId int64
	Nick      string `sql:",type=VARCHAR,size=32"`
}

func TestCoreNamerRoundTrip(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "Core_Named_Profile"`).Exec(ctx)
	_ = psql.Q(`DROP TABLE IF EXISTS "CoreNamedProfile"`).Exec(ctx)
	defer func() {
		_ = psql.Q(`DROP TABLE IF EXISTS "Core_Named_Profile"`).Exec(ctx)
		_ = psql.Q(`DROP TABLE IF EXISTS "CoreNamedProfile"`).Exec(ctx)
	}()

	tm := psql.Table[CoreNamedProfile]()
	assert.Equal(t, "CoreNamedProfile", tm.Name())
	assert.Equal(t, "Core_Named_Profile", tm.FormattedName(be), "LegacyNamer is the default")

	// LegacyNamer (default): Camel_Snake table, columns untouched
	require.NoError(t, psql.Insert(ctx, &CoreNamedProfile{ProfileId: 1, Nick: "legacy"}))
	var n int
	require.NoError(t, psql.Q(`SELECT COUNT(1) FROM "Core_Named_Profile" WHERE "ProfileId" = 1`).Each(ctx, func(r *sql.Rows) error { return r.Scan(&n) }))
	assert.Equal(t, 1, n)

	// DefaultNamer: Go names kept
	def := psql.NewBackend(be.Engine(), be.DB(), psql.WithNamer(&psql.DefaultNamer{}), psql.WithDriverData(be.DriverData()))
	dctx := def.Plug(context.Background())
	assert.Equal(t, "CoreNamedProfile", tm.FormattedName(def))
	require.NoError(t, psql.Insert(dctx, &CoreNamedProfile{ProfileId: 2, Nick: "default"}))
	got, err := psql.Get[CoreNamedProfile](dctx, map[string]any{"ProfileId": 2})
	require.NoError(t, err)
	assert.Equal(t, "default", got.Nick)
	require.NoError(t, psql.Q(`SELECT COUNT(1) FROM "CoreNamedProfile"`).Each(dctx, func(r *sql.Rows) error { return r.Scan(&n) }))
	assert.Equal(t, 1, n)
}

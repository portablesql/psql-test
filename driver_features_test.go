package ptest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/portablesql/psql"
	pgsql "github.com/portablesql/psql-pgsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for the driver side of the advanced features: RETURNING
// through the builder, CTEs, DISTINCT ON, JSON helpers, full-text and GIN
// keys created by the schema checker, identity columns, COPY bulk inserts,
// named locks, retry classification, LISTEN/NOTIFY, native PostgreSQL types
// and CockroachDB table options. Every test runs on the engine selected by
// PSQL_TEST_DSN (SQLite in memory by default) and skips what the product
// does not support. Each test owns its df_-prefixed table.

// dfDrop drops the named table, ignoring errors.
func dfDrop(ctx context.Context, name string) {
	_ = psql.Q(`DROP TABLE IF EXISTS "` + name + `"`).Exec(ctx)
}

// dfFresh returns a second backend on the same database, whose schema check
// state is empty, so that CheckStructure runs again on an existing table.
// Only meaningful for server databases: an in-memory SQLite backend is a new
// database.
func dfFresh(t *testing.T, be *psql.Backend) (*psql.Backend, context.Context) {
	t.Helper()
	if be.Engine() == psql.EngineSQLite {
		return be, be.Plug(context.Background())
	}
	be2 := getTestBackend(t)
	return be2, be2.Plug(context.Background())
}

// warnCapture is a slog handler recording the "event" attribute of every
// warning, used to check that the schema check does not complain.
type warnCapture struct {
	slog.Handler
	mu     sync.Mutex
	events []string
}

func (h *warnCapture) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "event" {
				h.mu.Lock()
				h.events = append(h.events, a.Value.String()+": "+r.Message)
				h.mu.Unlock()
			}
			return true
		})
	}
	return h.Handler.Handle(ctx, r)
}

func (h *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCapture) WithGroup(string) slog.Handler      { return h }

// captureWarnings installs a warning recorder as the default logger for the
// rest of the test. The recorder writes through its own text handler rather
// than wrapping slog's built-in default handler: that one writes through the
// log package, which slog.SetDefault redirects to the default logger, and
// the recursion deadlocks on log's mutex.
func captureWarnings(t *testing.T) *warnCapture {
	t.Helper()
	old := slog.Default()
	h := &warnCapture{Handler: slog.NewTextHandler(os.Stderr, nil)}
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() {
		slog.SetDefault(old)
		// SetDefault with the built-in handler leaves log's output on the
		// previous slog handler: put the standard destination back
		log.SetOutput(os.Stderr)
		log.SetFlags(log.LstdFlags)
	})
	return h
}

func (h *warnCapture) matching(prefix string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var res []string
	for _, e := range h.events {
		if strings.HasPrefix(e, prefix) {
			res = append(res, e)
		}
	}
	return res
}

// ---------------------------------------------------------------------------
// RETURNING through the builder and RunQueryT
// ---------------------------------------------------------------------------

type DfReturningItem struct {
	psql.Name `sql:"df_returning"`
	ID        int64  `sql:",key=PRIMARY,autoinc"`
	Label     string `sql:",type=VARCHAR,size=64"`
	Hits      int64
}

func TestDriverReturning(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_returning")
	defer dfDrop(ctx, "df_returning")

	// the schema check creates the table; Insert populates the key (through
	// RETURNING on PostgreSQL and SQLite, LastInsertId on MySQL/MariaDB)
	seed := &DfReturningItem{Label: "seed", Hits: 5}
	require.NoError(t, psql.Insert(ctx, seed))
	assert.NotZero(t, seed.ID)

	if !be.Supports(psql.FeatureReturning) {
		_, err := psql.B().Insert().Table("df_returning").Set(map[string]any{"Label": "x", "Hits": 1}).Returning("*").Render(ctx)
		assert.ErrorIs(t, err, psql.ErrNotSupported)
		t.Skipf("RETURNING not supported on %s", be.Variant())
	}

	// INSERT ... RETURNING *: the generated id comes back
	ins := psql.B().Insert().Table("df_returning").Set(map[string]any{"Label": "inserted", "Hits": 7}).Returning("*")
	got, err := psql.RunQueryTOne[DfReturningItem](ctx, ins)
	require.NoError(t, err)
	assert.NotZero(t, got.ID)
	assert.NotEqual(t, seed.ID, got.ID)
	assert.Equal(t, "inserted", got.Label)
	assert.Equal(t, int64(7), got.Hits)

	// UPDATE ... RETURNING (not on MariaDB)
	upd := psql.B().Update("df_returning").Set(map[string]any{"Hits": 8}).Where(map[string]any{"ID": got.ID}).Returning("ID", "Hits")
	if be.Variant() == psql.VariantMariaDB {
		_, err := upd.Render(ctx)
		assert.ErrorIs(t, err, psql.ErrNotSupported)
	} else {
		rows, err := psql.RunQueryT[DfReturningItem](ctx, upd)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, got.ID, rows[0].ID)
		assert.Equal(t, int64(8), rows[0].Hits)
	}

	// DELETE ... RETURNING through RunQuery
	del := psql.B().Delete().From("df_returning").Where(map[string]any{"Label": "inserted"}).Returning("ID", "Label")
	res, err := del.RunQuery(ctx)
	require.NoError(t, err)
	var ids []int64
	for res.Next() {
		var id int64
		var label string
		require.NoError(t, res.Scan(&id, &label))
		assert.Equal(t, "inserted", label)
		ids = append(ids, id)
	}
	require.NoError(t, res.Close())
	assert.Equal(t, []int64{got.ID}, ids)

	cnt, err := psql.Count[DfReturningItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// ---------------------------------------------------------------------------
// CTE
// ---------------------------------------------------------------------------

type DfCteItem struct {
	psql.Name `sql:"df_cte"`
	ID        int64 `sql:",key=PRIMARY"`
	Score     int64
}

func TestDriverCTE(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureCTE) {
		t.Skipf("CTEs not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_cte")
	defer dfDrop(ctx, "df_cte")
	for i := int64(1); i <= 6; i++ {
		require.NoError(t, psql.Insert(ctx, &DfCteItem{ID: i, Score: i * 10}))
	}

	high := psql.B().Select("ID", "Score").From("df_cte").Where(psql.Gt(psql.F("Score"), 30))
	q := psql.B().With("high", high).Select("*").From("high").OrderBy(psql.S("ID", "ASC"))
	sqlText, err := q.Render(ctx)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(sqlText, `WITH "high" AS (`), sqlText)

	rows, err := psql.RunQueryT[DfCteItem](ctx, q)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, int64(4), rows[0].ID)
	assert.Equal(t, int64(60), rows[2].Score)
}

// ---------------------------------------------------------------------------
// DISTINCT ON
// ---------------------------------------------------------------------------

type DfDistinctItem struct {
	psql.Name `sql:"df_distinct"`
	ID        int64  `sql:",key=PRIMARY"`
	Grp       string `sql:",type=VARCHAR,size=16"`
	Score     int64
}

func TestDriverDistinctOn(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	q := psql.B().DistinctOn("Grp").Select("*").From("df_distinct").OrderBy(psql.S("Grp", "ASC"), psql.S("Score", "DESC"))
	if be.Engine() != psql.EnginePostgreSQL {
		_, err := q.Render(ctx)
		assert.ErrorIs(t, err, psql.ErrNotSupported)
		t.Skipf("DISTINCT ON not supported on %s", be.Variant())
	}
	dfDrop(ctx, "df_distinct")
	defer dfDrop(ctx, "df_distinct")
	require.NoError(t, psql.Insert(ctx,
		&DfDistinctItem{ID: 1, Grp: "a", Score: 1},
		&DfDistinctItem{ID: 2, Grp: "a", Score: 9},
		&DfDistinctItem{ID: 3, Grp: "b", Score: 4},
		&DfDistinctItem{ID: 4, Grp: "b", Score: 2},
	))

	sqlText, err := q.Render(ctx)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(sqlText, `SELECT DISTINCT ON ("Grp")`), sqlText)
	rows, err := psql.RunQueryT[DfDistinctItem](ctx, q)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(2), rows[0].ID, "highest score of group a")
	assert.Equal(t, int64(3), rows[1].ID, "highest score of group b")
}

// ---------------------------------------------------------------------------
// JSON helpers on a format=json column
// ---------------------------------------------------------------------------

type DfJSONItem struct {
	psql.Name `sql:"df_json"`
	ID        int64          `sql:",key=PRIMARY"`
	Data      map[string]any `sql:",import=JSON"`
}

func TestDriverJSONHelpers(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureJSON) {
		t.Skipf("JSON not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_json")
	defer dfDrop(ctx, "df_json")

	alice := &DfJSONItem{ID: 1, Data: map[string]any{"name": "alice", "role": "admin", "tags": []any{"go", "sql"}, "prefs": map[string]any{"theme": "light"}}}
	bob := &DfJSONItem{ID: 2, Data: map[string]any{"name": "bob", "role": "user"}}
	require.NoError(t, psql.Insert(ctx, alice, bob))

	// round trip
	got, err := psql.Get[DfJSONItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	assert.Equal(t, "alice", got.Data["name"])
	assert.Equal(t, []any{"go", "sql"}, got.Data["tags"])

	// path extraction as text in a WHERE clause
	rows, err := psql.RunQueryT[DfJSONItem](ctx, psql.B().Select("*").From("df_json").Where(psql.Equal(psql.JSONGetText("Data", "name"), "bob")))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0].ID)

	rows, err = psql.RunQueryT[DfJSONItem](ctx, psql.B().Select("*").From("df_json").Where(psql.Equal(psql.JSONGetText("Data", "prefs", "theme"), "light")))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1), rows[0].ID)

	// key existence
	rows, err = psql.RunQueryT[DfJSONItem](ctx, psql.B().Select("*").From("df_json").Where(psql.JSONHasKey("Data", "tags")))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1), rows[0].ID)

	// containment (no operator on SQLite)
	contains := psql.B().Select("*").From("df_json").Where(psql.JSONContains("Data", map[string]any{"role": "user"}))
	if be.Engine() == psql.EngineSQLite {
		_, err := contains.Render(ctx)
		assert.ErrorIs(t, err, psql.ErrNotSupported)
	} else {
		rows, err = psql.RunQueryT[DfJSONItem](ctx, contains)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(2), rows[0].ID)
	}

	// in-place update of one path
	_, err = psql.B().Update("df_json").
		Set(map[string]any{"Data": psql.JSONSet("Data", []string{"role"}, "\"owner\"")}).
		Where(map[string]any{"ID": 2}).ExecQuery(ctx)
	require.NoError(t, err)
	got, err = psql.Get[DfJSONItem](ctx, map[string]any{"ID": 2})
	require.NoError(t, err)
	assert.Equal(t, "owner", got.Data["role"])
	assert.Equal(t, "bob", got.Data["name"], "other keys are kept")
}

// ---------------------------------------------------------------------------
// Full-text search with a FULLTEXT key created by the schema checker
// ---------------------------------------------------------------------------

type DfFulltextItem struct {
	psql.Name `sql:"df_fulltext"`
	ID        int64    `sql:",key=PRIMARY"`
	Title     string   `sql:",type=VARCHAR,size=128"`
	Body      string   `sql:",type=TEXT"`
	FT        psql.Key `sql:",type=FULLTEXT,fields='Title,Body'"`
}

// dfFulltextIndexed reports whether the FT key of df_fulltext exists, with
// its definition (PostgreSQL) or type (MySQL).
func dfFulltextIndexed(t *testing.T, ctx context.Context, be *psql.Backend) (bool, string) {
	t.Helper()
	var def string
	var found bool
	var err error
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		err = psql.Q(`SELECT indexdef FROM pg_indexes WHERE tablename = 'df_fulltext' AND indexname = 'df_fulltext_FT'`).Each(ctx, func(rows *sql.Rows) error {
			found = true
			return rows.Scan(&def)
		})
	case psql.EngineMySQL:
		err = psql.Q(`SELECT index_type FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'df_fulltext' AND index_name = 'FT' LIMIT 1`).Each(ctx, func(rows *sql.Rows) error {
			found = true
			return rows.Scan(&def)
		})
	}
	require.NoError(t, err)
	return found, def
}

func TestDriverFullText(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	if !be.Supports(psql.FeatureFullText) {
		_, err := psql.B().Select("*").From("df_fulltext").Where(psql.FullText("go", "Title", "Body")).Render(ctx)
		assert.ErrorIs(t, err, psql.ErrNotSupported)
		t.Skipf("full-text search not supported on %s", be.Variant())
	}
	dfDrop(ctx, "df_fulltext")
	defer dfDrop(ctx, "df_fulltext")

	require.NoError(t, psql.Insert(ctx,
		&DfFulltextItem{ID: 1, Title: "Go generics explained", Body: "Type parameters arrived in Go 1.18"},
		&DfFulltextItem{ID: 2, Title: "Cooking pasta", Body: "Boil water, add salt"},
		&DfFulltextItem{ID: 3, Title: "Rust traits", Body: "Generic programming with traits and generics"},
	))

	found, def := dfFulltextIndexed(t, ctx, be)
	require.True(t, found, "the schema check must create the FULLTEXT key")
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		assert.Contains(t, strings.ToLower(def), "gin", def)
		assert.Contains(t, def, "to_tsvector", def)
	case psql.EngineMySQL:
		assert.Equal(t, "FULLTEXT", def)
	}

	search := func(query string, extra ...any) []int64 {
		args := append([]any{"Title", "Body"}, extra...)
		rank := psql.FullTextRank(query, args...)
		rows, err := psql.RunQueryT[DfFulltextItem](ctx, psql.B().Select("*").From("df_fulltext").
			Where(psql.FullText(query, args...)).OrderBy(rank.Desc(), psql.S("ID", "ASC")))
		require.NoError(t, err)
		ids := make([]int64, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		return ids
	}
	assert.ElementsMatch(t, []int64{1, 3}, search("generics"))
	assert.Equal(t, []int64{2}, search("pasta"))
	assert.Empty(t, search("nothing"))
	assert.Equal(t, []int64{2}, search("water salt", psql.FullTextBoolean))

	// the checker also creates the key on an existing table
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		require.NoError(t, psql.Q(`DROP INDEX "df_fulltext_FT"`).Exec(ctx))
	case psql.EngineMySQL:
		require.NoError(t, psql.Q(`ALTER TABLE "df_fulltext" DROP INDEX "FT"`).Exec(ctx))
	}
	found, _ = dfFulltextIndexed(t, ctx, be)
	require.False(t, found)
	be2, ctx2 := dfFresh(t, be)
	require.NoError(t, be2.CheckStructure(ctx2, psql.Table[DfFulltextItem]()))
	found, _ = dfFulltextIndexed(t, ctx2, be2)
	assert.True(t, found, "CheckStructure must recreate the FULLTEXT key")
	assert.ElementsMatch(t, []int64{1, 3}, search("generics"))
}

// ---------------------------------------------------------------------------
// GIN indexes on a jsonb column (PostgreSQL / CockroachDB)
// ---------------------------------------------------------------------------

type DfGinItem struct {
	psql.Name `sql:"df_gin"`
	ID        int64          `sql:",key=PRIMARY"`
	Title     string         `sql:",type=VARCHAR,size=128"`
	Data      map[string]any `sql:",import=JSON"`
	DataIdx   psql.Key       `sql:",type=GIN,fields='Data'"`
	TitleIdx  psql.Key       `sql:",type=GIN,expression=\"to_tsvector('simple', {Title})\""`
}

func dfIndexDefs(t *testing.T, ctx context.Context, table string) map[string]string {
	t.Helper()
	defs := map[string]string{}
	require.NoError(t, psql.Q(`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1`, table).Each(ctx, func(rows *sql.Rows) error {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return err
		}
		defs[name] = def
		return nil
	}))
	return defs
}

func TestDriverGINIndex(t *testing.T) {
	be := getTestBackend(t)
	if be.Engine() != psql.EnginePostgreSQL {
		t.Skip("GIN indexes are PostgreSQL/CockroachDB only")
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_gin")
	defer dfDrop(ctx, "df_gin")

	require.NoError(t, psql.Insert(ctx,
		&DfGinItem{ID: 1, Title: "first post", Data: map[string]any{"tags": []any{"go"}, "n": float64(1)}},
		&DfGinItem{ID: 2, Title: "second post", Data: map[string]any{"tags": []any{"sql"}, "n": float64(2)}},
	))
	defs := dfIndexDefs(t, ctx, "df_gin")
	require.Contains(t, defs, "df_gin_DataIdx")
	assert.Contains(t, strings.ToLower(defs["df_gin_DataIdx"]), "gin", defs["df_gin_DataIdx"])
	require.Contains(t, defs, "df_gin_TitleIdx")
	assert.Contains(t, defs["df_gin_TitleIdx"], "to_tsvector", defs["df_gin_TitleIdx"])

	// the indexed containment query works
	rows, err := psql.RunQueryT[DfGinItem](ctx, psql.B().Select("*").From("df_gin").Where(psql.JSONContains("Data", map[string]any{"tags": []string{"sql"}})))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0].ID)

	// missing GIN indexes are created on an existing table
	require.NoError(t, psql.Q(`DROP INDEX "df_gin_DataIdx"`).Exec(ctx))
	require.NoError(t, psql.Q(`DROP INDEX "df_gin_TitleIdx"`).Exec(ctx))
	be2, ctx2 := dfFresh(t, be)
	require.NoError(t, be2.CheckStructure(ctx2, psql.Table[DfGinItem]()))
	defs = dfIndexDefs(t, ctx2, "df_gin")
	assert.Contains(t, defs, "df_gin_DataIdx")
	assert.Contains(t, defs, "df_gin_TitleIdx")
}

// ---------------------------------------------------------------------------
// autoinc columns rendered by the schema checker
// ---------------------------------------------------------------------------

type DfIdentityItem struct {
	psql.Name `sql:"df_identity"`
	ID        uint64 `sql:",key=PRIMARY,autoinc"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestDriverAutoIncIdentity(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_identity")
	defer dfDrop(ctx, "df_identity")
	warnings := captureWarnings(t)

	// the table is created by the schema check with the driver's identity
	// rendering: GENERATED BY DEFAULT AS IDENTITY, AUTO_INCREMENT, integer
	a := &DfIdentityItem{Label: "a"}
	b := &DfIdentityItem{Label: "b"}
	require.NoError(t, psql.Insert(ctx, a))
	require.NoError(t, psql.Insert(ctx, b))
	assert.NotZero(t, a.ID)
	assert.NotZero(t, b.ID)
	assert.NotEqual(t, a.ID, b.ID)
	c := &DfIdentityItem{ID: 500, Label: "c"}
	require.NoError(t, psql.Insert(ctx, c))
	assert.Equal(t, uint64(500), c.ID)

	// the column really is generated by the database
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		var identity string
		require.NoError(t, psql.Q(`SELECT is_identity FROM information_schema.columns WHERE table_name = 'df_identity' AND column_name = 'ID'`).Each(ctx, func(rows *sql.Rows) error {
			return rows.Scan(&identity)
		}))
		assert.Equal(t, "YES", identity)
	case psql.EngineMySQL:
		var extra string
		require.NoError(t, psql.Q(`SELECT extra FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'df_identity' AND column_name = 'ID'`).Each(ctx, func(rows *sql.Rows) error {
			return rows.Scan(&extra)
		}))
		assert.Contains(t, strings.ToLower(extra), "auto_increment")
	case psql.EngineSQLite:
		var ddl string
		require.NoError(t, psql.Q(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'df_identity'`).Each(ctx, func(rows *sql.Rows) error {
			return rows.Scan(&ddl)
		}))
		assert.Contains(t, ddl, `"ID" integer`, ddl)
	}

	// a second schema check sees the identity column as matching: no
	// column mismatch warning and no ALTER
	be2, ctx2 := dfFresh(t, be)
	require.NoError(t, be2.CheckStructure(ctx2, psql.Table[DfIdentityItem]()))
	assert.Empty(t, warnings.matching("psql:check:column_mismatch"))
	d := &DfIdentityItem{Label: "d"}
	require.NoError(t, psql.Insert(ctx2, d))
	assert.NotZero(t, d.ID)
	assert.NotEqual(t, uint64(500), d.ID)
}

// ---------------------------------------------------------------------------
// BulkInsert through COPY (PostgreSQL) and its fallback
// ---------------------------------------------------------------------------

type DfBulkItem struct {
	psql.Name `sql:"df_bulk"`
	ID        int64   `sql:",key=PRIMARY"`
	Label     string  `sql:",type=VARCHAR,size=64"`
	Score     float64 `sql:",null=1"`
	Active    bool
	Data      map[string]any `sql:",import=JSON"`
	Blob      []byte
	At        time.Time
	Note      *string `sql:",type=TEXT"`
}

func TestDriverBulkInsertCopy(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureBulkCopy) {
		t.Skipf("native bulk loading not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_bulk")
	defer dfDrop(ctx, "df_bulk")
	require.NoError(t, psql.Insert(ctx, &DfBulkItem{ID: 0, Label: "schema"}))
	_, err := psql.Delete[DfBulkItem](ctx, map[string]any{"ID": 0})
	require.NoError(t, err)

	note := "with \\ backslash,\ttab and\nnewline"
	when := time.Date(2024, 3, 5, 12, 30, 0, 123456000, time.UTC)
	const n = 2000
	rows := make([]*DfBulkItem, n)
	for i := range rows {
		rows[i] = &DfBulkItem{
			ID:     int64(i + 1),
			Label:  fmt.Sprintf("row-%04d", i),
			Score:  float64(i) / 4,
			Active: i%2 == 0,
			Data:   map[string]any{"i": float64(i), "tags": []any{"a", "b"}},
			Blob:   []byte{byte(i), 0, '\\', '\n', 0xff},
			At:     when.Add(time.Duration(i) * time.Second),
		}
		if i%3 == 0 {
			rows[i].Note = &note
		}
	}
	rows[5].Label = "tab\there \\ and \"quotes\""
	require.NoError(t, psql.BulkInsert(ctx, rows))

	cnt, err := psql.Count[DfBulkItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, n, cnt)

	for _, i := range []int{0, 5, 6, n - 1} {
		got, err := psql.Get[DfBulkItem](ctx, map[string]any{"ID": rows[i].ID})
		require.NoError(t, err)
		assert.Equal(t, rows[i].Label, got.Label)
		assert.Equal(t, rows[i].Score, got.Score)
		assert.Equal(t, rows[i].Active, got.Active)
		assert.Equal(t, rows[i].Data, got.Data)
		assert.Equal(t, rows[i].Blob, got.Blob)
		assert.True(t, rows[i].At.Equal(got.At), "At: %s != %s", rows[i].At, got.At)
		if rows[i].Note == nil {
			assert.Nil(t, got.Note)
		} else {
			require.NotNil(t, got.Note)
			assert.Equal(t, note, *got.Note)
		}
	}

	// a failing COPY reports the COPY statement, which proves the native
	// path was taken; inside a transaction the multi-row INSERT fallback is
	// used and reports the INSERT statement
	err = psql.BulkInsert(ctx, []*DfBulkItem{{ID: 1, Label: "dup"}})
	require.Error(t, err)
	var qerr *psql.Error
	require.ErrorAs(t, err, &qerr)
	assert.True(t, strings.HasPrefix(qerr.Query, "COPY "), qerr.Query)
	assert.True(t, psql.IsDuplicate(err))

	err = psql.Tx(ctx, func(tx context.Context) error {
		if err := psql.BulkInsert(tx, []*DfBulkItem{{ID: n + 1, Label: "tx"}, {ID: n + 2, Label: "tx"}}); err != nil {
			return err
		}
		c, err := psql.Count[DfBulkItem](tx, nil)
		if err != nil {
			return err
		}
		if c != n+2 {
			return fmt.Errorf("count inside the transaction = %d", c)
		}
		err = psql.BulkInsert(tx, []*DfBulkItem{{ID: 1, Label: "dup"}})
		var qerr *psql.Error
		if !errors.As(err, &qerr) || !strings.HasPrefix(qerr.Query, "INSERT ") {
			return fmt.Errorf("expected the INSERT fallback inside a transaction, got %v", err)
		}
		return errors.New("rollback")
	})
	assert.EqualError(t, err, "rollback")
	cnt, err = psql.Count[DfBulkItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, n, cnt, "the transaction was rolled back")
}

type DfBulkVecItem struct {
	psql.Name `sql:"df_bulk_vec"`
	ID        int64       `sql:",key=PRIMARY"`
	Embedding psql.Vector `sql:",size=3"`
}

func TestDriverBulkInsertVector(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureBulkCopy) {
		t.Skipf("native bulk loading not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())
	_ = psql.Q("CREATE EXTENSION IF NOT EXISTS vector").Exec(ctx)
	dfDrop(ctx, "df_bulk_vec")
	defer dfDrop(ctx, "df_bulk_vec")
	if err := psql.Insert(ctx, &DfBulkVecItem{ID: 0, Embedding: psql.Vector{0, 0, 0}}); err != nil {
		t.Skipf("vector type not available: %v", err)
	}

	rows := make([]*DfBulkVecItem, 50)
	for i := range rows {
		rows[i] = &DfBulkVecItem{ID: int64(i + 1), Embedding: psql.Vector{float32(i), 0.5, -1}}
	}
	require.NoError(t, psql.BulkInsert(ctx, rows))
	got, err := psql.Get[DfBulkVecItem](ctx, map[string]any{"ID": 50})
	require.NoError(t, err)
	require.Equal(t, 3, got.Embedding.Dimensions())
	assert.InDelta(t, 49, got.Embedding[0], 0.001)
	assert.InDelta(t, 0.5, got.Embedding[1], 0.001)
	assert.InDelta(t, -1, got.Embedding[2], 0.001)
	cnt, err := psql.Count[DfBulkVecItem](ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 51, cnt)
}

// ---------------------------------------------------------------------------
// Named locks: timeout across connections
// ---------------------------------------------------------------------------

func TestDriverNamedLockTimeout(t *testing.T) {
	be := getTestBackend(t)
	if !be.Supports(psql.FeatureAdvisoryLocks) {
		t.Skipf("named locks not supported on %s", be.Variant())
	}
	ctx := be.Plug(context.Background())

	release, err := psql.NamedLock(ctx, "df-timeout", 0)
	require.NoError(t, err)

	// another connection waits for the timeout, then gives up
	start := time.Now()
	_, err = psql.NamedLock(ctx, "df-timeout", 300*time.Millisecond)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.ErrorIs(t, err, psql.ErrLockTimeout)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "the wait must honour the timeout")
	assert.Less(t, elapsed, 5*time.Second)

	require.NoError(t, release())

	// free again: the timed wait succeeds immediately
	start = time.Now()
	err = psql.WithNamedLock(ctx, "df-timeout", 2*time.Second, func(context.Context) error { return nil })
	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second)
}

// ---------------------------------------------------------------------------
// Retry classification with synthetic errors
// ---------------------------------------------------------------------------

// dfRetryableError returns a driver error the current engine classifies as
// retryable.
func dfRetryableError(be *psql.Backend) error {
	switch be.Engine() {
	case psql.EnginePostgreSQL:
		return &pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update"}
	case psql.EngineMySQL:
		return &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	default:
		return errors.New("database is locked (5) (SQLITE_BUSY)")
	}
}

func TestDriverRetryClassification(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())

	// every registered dialect answers, whatever the current engine
	retryable := []error{
		&pgconn.PgError{Code: "40001", Message: "could not serialize access"},
		&pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
		&pgconn.PgError{Code: "40001", Message: "restart transaction: TransactionRetryWithProtoRefreshError"},
		&mysql.MySQLError{Number: 1213, Message: "Deadlock found"},
		&mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"},
		errors.New("database is locked (5) (SQLITE_BUSY)"),
		errors.New("database table is locked (6) (SQLITE_LOCKED)"),
	}
	for _, err := range retryable {
		assert.True(t, psql.IsRetryable(err), "%v", err)
		assert.True(t, psql.IsRetryable(&psql.Error{Query: "UPDATE", Err: err}), "wrapped %v", err)
		assert.True(t, psql.IsRetryable(fmt.Errorf("commit: %w", err)), "fmt wrapped %v", err)
		assert.True(t, psql.IsRetryable(errors.Join(errors.New("other"), err)), "joined %v", err)
	}
	for _, err := range []error{
		nil,
		errors.New("plain"),
		&pgconn.PgError{Code: "23505", Message: "duplicate key"},
		&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"},
		errors.New("UNIQUE constraint failed: t.id"),
		context.Canceled,
	} {
		assert.False(t, psql.IsRetryable(err), "%v", err)
	}

	// TxWithOptions retries when the callback fails with the engine's
	// retryable error, and gives up after MaxRetries
	calls := 0
	opts := &psql.TxOptions{MaxRetries: 2, Backoff: func(int) time.Duration { return 0 }}
	err := psql.TxWithOptions(ctx, opts, func(context.Context) error {
		calls++
		if calls == 1 {
			return &psql.Error{Query: "UPDATE", Err: dfRetryableError(be)}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "one retry after the transient failure")

	calls = 0
	err = psql.TxWithOptions(ctx, opts, func(context.Context) error {
		calls++
		return dfRetryableError(be)
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, psql.ErrTxRetriesExhausted)
	assert.True(t, psql.IsRetryable(err), "the last error stays visible: %v", err)
	assert.Equal(t, 3, calls, "first attempt plus MaxRetries")
}

// ---------------------------------------------------------------------------
// LISTEN / NOTIFY (PostgreSQL)
// ---------------------------------------------------------------------------

func TestDriverListenNotify(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	if !be.Supports(psql.FeatureListenNotify) {
		if be.Engine() == psql.EnginePostgreSQL {
			assert.ErrorIs(t, pgsql.Notify(ctx, "df_channel", "x"), psql.ErrNotSupported)
			assert.ErrorIs(t, pgsql.Listen(ctx, be, "df_channel", func(pgsql.Notification) {}), psql.ErrNotSupported)
		}
		t.Skipf("LISTEN/NOTIFY not supported on %s", be.Variant())
	}
	lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	received := make(chan pgsql.Notification, 256)
	done := make(chan error, 1)
	go func() {
		done <- pgsql.Listen(lctx, be, "df_channel", func(n pgsql.Notification) { received <- n })
	}()

	// LISTEN is issued asynchronously: notify until the first one arrives
	var first pgsql.Notification
	deadline := time.Now().Add(10 * time.Second)
wait:
	for {
		require.NoError(t, pgsql.Notify(ctx, "df_channel", "hello"))
		select {
		case first = <-received:
			break wait
		case <-time.After(200 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("no notification received")
			}
		}
	}
	assert.Equal(t, "df_channel", first.Channel)
	assert.Equal(t, "hello", first.Payload)
	assert.NotZero(t, first.PID)

	// notifications follow the transaction: dropped on rollback, delivered
	// on commit
	err := psql.Tx(ctx, func(tx context.Context) error {
		if err := pgsql.Notify(tx, "df_channel", "rolled back"); err != nil {
			return err
		}
		return errors.New("abort")
	})
	assert.EqualError(t, err, "abort")
	require.NoError(t, psql.Tx(ctx, func(tx context.Context) error {
		return pgsql.Notify(tx, "df_channel", "committed")
	}))
	var payloads []string
	for {
		select {
		case n := <-received:
			payloads = append(payloads, n.Payload)
			if n.Payload == "committed" {
				goto collected
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("committed notification not received, got %v", payloads)
		}
	}
collected:
	assert.NotContains(t, payloads, "rolled back")

	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Listen did not return after cancellation")
	}
}

// ---------------------------------------------------------------------------
// Native PostgreSQL types
// ---------------------------------------------------------------------------

type DfNativeItem struct {
	psql.Name `sql:"df_native"`
	ID        string    `sql:",import=UUID,key=PRIMARY"`
	Addr      string    `sql:",import=IP,null=1"`
	Net       string    `sql:",import=CIDR,null=1"`
	At        time.Time `sql:",import=TIMESTAMPTZ"`
	Legacy    string    `sql:",type=CHAR,size=36"`
}

func TestDriverNativeTypes(t *testing.T) {
	be := getTestBackend(t)
	if be.Engine() != psql.EnginePostgreSQL {
		t.Skip("native types are PostgreSQL/CockroachDB only")
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_native")
	defer dfDrop(ctx, "df_native")
	warnings := captureWarnings(t)

	when := time.Date(2024, 3, 5, 12, 30, 0, 123456000, time.FixedZone("JST", 9*3600))
	item := &DfNativeItem{ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Addr: "192.0.2.10", Net: "10.0.0.0/8", At: when, Legacy: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}
	require.NoError(t, psql.Insert(ctx, item))

	types := map[string]string{}
	require.NoError(t, psql.Q(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'df_native'`).Each(ctx, func(rows *sql.Rows) error {
		var col, typ string
		if err := rows.Scan(&col, &typ); err != nil {
			return err
		}
		types[col] = typ
		return nil
	}))
	assert.Equal(t, "uuid", types["ID"])
	assert.Equal(t, "inet", types["Addr"])
	assert.Equal(t, "cidr", types["Net"])
	assert.Equal(t, "timestamp with time zone", types["At"])
	assert.Equal(t, "character", types["Legacy"])

	got, err := psql.Get[DfNativeItem](ctx, map[string]any{"ID": item.ID})
	require.NoError(t, err)
	assert.Equal(t, item.ID, got.ID)
	assert.Equal(t, "192.0.2.10", got.Addr)
	assert.Equal(t, "10.0.0.0/8", got.Net)
	assert.True(t, when.Equal(got.At), "At: %s != %s", when, got.At)
	assert.Equal(t, item.Legacy, got.Legacy)

	// the schema check recognizes the native types and char(36) as matching
	be2, ctx2 := dfFresh(t, be)
	require.NoError(t, be2.CheckStructure(ctx2, psql.Table[DfNativeItem]()))
	assert.Empty(t, warnings.matching("psql:check:column_mismatch"))
}

// ---------------------------------------------------------------------------
// CockroachDB table options and CockroachDB-only behaviour
// ---------------------------------------------------------------------------

type DfTTLItem struct {
	psql.Name `sql:"df_ttl,ttl_expire_after='30 days'"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

func TestDriverTableOptionsOnPostgreSQL(t *testing.T) {
	be := getTestBackend(t)
	if be.Variant() != psql.VariantPostgreSQL {
		t.Skip("checks that CockroachDB table options are refused on PostgreSQL")
	}
	ctx := be.Plug(context.Background())
	dfDrop(ctx, "df_ttl")
	assert.False(t, be.Supports(psql.FeatureRowTTL))
	err := be.CheckStructure(ctx, psql.Table[DfTTLItem]())
	require.Error(t, err)
	assert.ErrorIs(t, err, psql.ErrNotSupported)
	// the table was not created, so it cannot be used either
	require.Error(t, psql.Insert(ctx, &DfTTLItem{ID: 1, Label: "x"}))
}

func TestDriverCockroachDB(t *testing.T) {
	be := getTestBackend(t)
	if be.Variant() != psql.VariantCockroachDB {
		t.Skip("CockroachDB only")
	}
	ctx := be.Plug(context.Background())
	assert.True(t, be.Supports(psql.FeatureRowTTL))
	assert.True(t, be.Supports(psql.FeatureAsOfSystemTime))
	assert.False(t, be.Supports(psql.FeatureAdvisoryLocks))
	assert.False(t, be.Supports(psql.FeatureListenNotify))
	assert.Contains(t, be.ServerVersion(), "CockroachDB")

	// row TTL table option
	dfDrop(ctx, "df_ttl")
	defer dfDrop(ctx, "df_ttl")
	require.NoError(t, psql.Insert(ctx, &DfTTLItem{ID: 1, Label: "x"}))
	var ddl string
	require.NoError(t, psql.Q(`SELECT create_statement FROM [SHOW CREATE TABLE "df_ttl"]`).Each(ctx, func(rows *sql.Rows) error {
		return rows.Scan(&ddl)
	}))
	assert.Contains(t, ddl, "ttl_expire_after", ddl)

	// AS OF SYSTEM TIME follower read
	time.Sleep(100 * time.Millisecond)
	rows, err := psql.RunQueryT[DfTTLItem](ctx, psql.B().Select("*").From("df_ttl").AsOfSystemTime("-50ms"))
	require.NoError(t, err)
	assert.Len(t, rows, 1)

	// no named locks, no LISTEN/NOTIFY
	_, err = psql.NamedLock(ctx, "df-crdb", 0)
	assert.ErrorIs(t, err, psql.ErrNotSupported)
	assert.ErrorIs(t, pgsql.Notify(ctx, "df_channel", "x"), psql.ErrNotSupported)
}

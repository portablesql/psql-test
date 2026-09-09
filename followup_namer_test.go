package ptest

import (
	"context"
	"database/sql"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Fn* types have no explicit table or column names, so under
// CamelSnakeNamer every SQL name is transformed: "FnAuthor" becomes
// "Fn_Author", "PenName" becomes "Pen_Name", "AuthorId" becomes "Author_Id".
// Association tags keep referring to Go names.

type FnAuthor struct {
	Id      int64      `sql:",key=PRIMARY"`
	PenName string     `sql:",type=VARCHAR,size=64"`
	Score   float64    `sql:",type=DOUBLE"`
	Books   []*FnBook  `psql:"has_many:AuthorId;order='PubYear DESC'"`
	Profile *FnProfile `psql:"has_one:AuthorId"`
	// join table given by Go type name, columns by Go field names
	Tags []*FnTag `psql:"many_to_many:FnAuthorTag,AuthorId,TagId;order='Label'"`
	// join table given by its resolved SQL names
	TagsSQL []*FnTag `psql:"many_to_many:Fn_Author_Tag,Author_Id,Tag_Id"`
}

type FnBook struct {
	Id       int64 `sql:",key=PRIMARY"`
	AuthorId int64
	Title    string    `sql:",type=VARCHAR,size=64"`
	PubYear  int       `sql:",type=INT"`
	Author   *FnAuthor `psql:"belongs_to:AuthorId"`
}

type FnProfile struct {
	Id       int64 `sql:",key=PRIMARY"`
	AuthorId int64
	Bio      string `sql:",type=VARCHAR,size=64"`
}

type FnTag struct {
	Id    int64  `sql:",key=PRIMARY"`
	Label string `sql:",type=VARCHAR,size=64"`
}

type FnAuthorTag struct {
	AuthorId int64 `sql:",key=PRIMARY"`
	TagId    int64 `sql:",key=PRIMARY"`
}

var fnTables = []string{"Fn_Author_Tag", "Fn_Tag", "Fn_Profile", "Fn_Book", "Fn_Author"}

// setupFnNamer returns a context bound to a backend using CamelSnakeNamer,
// with the Fn* tables created and populated.
func setupFnNamer(t *testing.T) context.Context {
	t.Helper()
	be := getTestBackend(t)
	be.SetNamer(&psql.CamelSnakeNamer{})
	ctx := be.Plug(context.Background())

	drop := func() {
		for _, n := range fnTables {
			_ = psql.Q(`DROP TABLE IF EXISTS ` + psql.QuoteName(n)).Exec(ctx)
		}
	}
	drop()
	t.Cleanup(drop)

	_ = psql.Table[FnAuthor]()
	_ = psql.Table[FnBook]()
	_ = psql.Table[FnProfile]()
	_ = psql.Table[FnTag]()
	_ = psql.Table[FnAuthorTag]()

	require.NoError(t, psql.Insert(ctx, &FnAuthor{Id: 1, PenName: "Alice", Score: 1.5}))
	require.NoError(t, psql.Insert(ctx, &FnAuthor{Id: 2, PenName: "Bob", Score: 2.5}))
	require.NoError(t, psql.Insert(ctx, &FnAuthor{Id: 3, PenName: "Carol", Score: 3.5}))

	require.NoError(t, psql.Insert(ctx, &FnBook{Id: 10, AuthorId: 1, Title: "First", PubYear: 2001}))
	require.NoError(t, psql.Insert(ctx, &FnBook{Id: 11, AuthorId: 1, Title: "Second", PubYear: 2005}))
	require.NoError(t, psql.Insert(ctx, &FnBook{Id: 12, AuthorId: 2, Title: "Third", PubYear: 2003}))

	require.NoError(t, psql.Insert(ctx, &FnProfile{Id: 100, AuthorId: 1, Bio: "bio a"}))
	require.NoError(t, psql.Insert(ctx, &FnProfile{Id: 101, AuthorId: 2, Bio: "bio b"}))

	require.NoError(t, psql.Insert(ctx, &FnTag{Id: 1, Label: "go"}))
	require.NoError(t, psql.Insert(ctx, &FnTag{Id: 2, Label: "sql"}))
	require.NoError(t, psql.Insert(ctx, &FnTag{Id: 3, Label: "db"}))

	// Alice's tags are inserted in reverse label order to check ordering
	require.NoError(t, psql.Insert(ctx, &FnAuthorTag{AuthorId: 1, TagId: 2}))
	require.NoError(t, psql.Insert(ctx, &FnAuthorTag{AuthorId: 1, TagId: 1}))
	require.NoError(t, psql.Insert(ctx, &FnAuthorTag{AuthorId: 2, TagId: 2}))
	require.NoError(t, psql.Insert(ctx, &FnAuthorTag{AuthorId: 3, TagId: 3}))

	// the schema really uses the transformed names
	var n int
	require.NoError(t, psql.Q(`SELECT COUNT(1) FROM "Fn_Book" WHERE "Author_Id" = 1`).Each(ctx, func(r *sql.Rows) error { return r.Scan(&n) }))
	require.Equal(t, 2, n)
	require.NoError(t, psql.Q(`SELECT COUNT(1) FROM "Fn_Author_Tag" WHERE "Tag_Id" = 2`).Each(ctx, func(r *sql.Rows) error { return r.Scan(&n) }))
	require.Equal(t, 2, n)

	return ctx
}

func fnAuthorsByName(authors []*FnAuthor) map[string]*FnAuthor {
	m := make(map[string]*FnAuthor, len(authors))
	for _, a := range authors {
		m[a.PenName] = a
	}
	return m
}

func fnLabels(tags []*FnTag) []string {
	res := make([]string, len(tags))
	for i, tg := range tags {
		res[i] = tg.Label
	}
	return res
}

func checkFnPreload(t *testing.T, ctx context.Context) {
	t.Helper()

	// has_many, has_one and many_to_many from the parent side
	authors, err := psql.Fetch[FnAuthor](ctx, nil, psql.WithPreload("Books", "Profile", "Tags", "TagsSQL"))
	require.NoError(t, err)
	require.Len(t, authors, 3)
	by := fnAuthorsByName(authors)

	alice, bob, carol := by["Alice"], by["Bob"], by["Carol"]
	require.NotNil(t, alice)
	require.NotNil(t, bob)
	require.NotNil(t, carol)

	require.Len(t, alice.Books, 2)
	assert.Equal(t, "Second", alice.Books[0].Title, "order='PubYear DESC' must resolve to Pub_Year")
	assert.Equal(t, "First", alice.Books[1].Title)
	require.Len(t, bob.Books, 1)
	assert.Equal(t, "Third", bob.Books[0].Title)
	assert.Empty(t, carol.Books)

	require.NotNil(t, alice.Profile)
	assert.Equal(t, "bio a", alice.Profile.Bio)
	require.NotNil(t, bob.Profile)
	assert.Equal(t, "bio b", bob.Profile.Bio)
	assert.Nil(t, carol.Profile)

	assert.Equal(t, []string{"go", "sql"}, fnLabels(alice.Tags), "order='Label' on many_to_many")
	assert.Equal(t, []string{"sql"}, fnLabels(bob.Tags))
	assert.Equal(t, []string{"db"}, fnLabels(carol.Tags))
	assert.ElementsMatch(t, []string{"go", "sql"}, fnLabels(alice.TagsSQL), "join table given by resolved SQL names")
	assert.ElementsMatch(t, []string{"db"}, fnLabels(carol.TagsSQL))

	// belongs_to from the child side; parents referencing the same author share a pointer
	books, err := psql.Fetch[FnBook](ctx, nil, psql.WithPreload("Author"))
	require.NoError(t, err)
	require.Len(t, books, 3)
	var aliceBooks []*FnBook
	for _, b := range books {
		require.NotNil(t, b.Author, "book %d", b.Id)
		assert.Equal(t, b.AuthorId, b.Author.Id)
		if b.AuthorId == 1 {
			aliceBooks = append(aliceBooks, b)
		}
	}
	require.Len(t, aliceBooks, 2)
	assert.Same(t, aliceBooks[0].Author, aliceBooks[1].Author)
	assert.Equal(t, "Alice", aliceBooks[0].Author.PenName)

	// PreloadOpts with the preload list carried by the options
	fresh, err := psql.Fetch[FnAuthor](ctx, map[string]any{"Id": 1})
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.NoError(t, psql.PreloadOpts(ctx, fresh, &psql.FetchOptions{Preload: []string{"Books", "Tags"}}))
	assert.Len(t, fresh[0].Books, 2)
	assert.Equal(t, []string{"go", "sql"}, fnLabels(fresh[0].Tags))
}

func TestFollowupPreloadCamelSnakeNamer(t *testing.T) {
	ctx := setupFnNamer(t)
	checkFnPreload(t, ctx)

	// same checks with chunked IN lists (several queries per association,
	// Go-side re-sorting of ordered many_to_many results)
	saved := psql.PreloadChunkSize
	psql.PreloadChunkSize = 1
	t.Cleanup(func() { psql.PreloadChunkSize = saved })
	checkFnPreload(t, ctx)
}

func TestFollowupLazyCamelSnakeNamer(t *testing.T) {
	ctx := setupFnNamer(t)

	// batched futures on the primary key (Go name == column name here)
	bctx := psql.WithLazyBatch(ctx)
	f1 := psql.LazyCtx[FnAuthor](bctx, "Id", "1")
	f2 := psql.LazyCtx[FnAuthor](bctx, "Id", "2")
	a1, err := f1.Resolve(bctx)
	require.NoError(t, err)
	assert.Equal(t, "Alice", a1.PenName)
	a2, err := f2.Resolve(bctx)
	require.NoError(t, err)
	assert.Equal(t, "Bob", a2.PenName)

	// column given as Go field name, resolved to "Pen_Name"
	f3 := psql.LazyCtx[FnAuthor](ctx, "PenName", "Carol")
	a3, err := f3.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), a3.Id)

	// column given as its resolved SQL name
	f4 := psql.LazyCtx[FnAuthor](ctx, "Pen_Name", "Alice")
	a4, err := f4.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), a4.Id)

	// registry (context-less) futures, resolved against the namer backend
	f5 := psql.Lazy[FnAuthor]("PenName", "Bob")
	a5, err := f5.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), a5.Id)

	// non-batchable column type (float): one query, resolved column name
	f6 := psql.LazyCtx[FnAuthor](ctx, "Score", "3.5")
	a6, err := f6.Resolve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Carol", a6.PenName)

	// missing record
	_, err = psql.LazyCtx[FnAuthor](ctx, "PenName", "Nobody").Resolve(ctx)
	require.Error(t, err)
	assert.True(t, psql.IsNotExist(err))
}

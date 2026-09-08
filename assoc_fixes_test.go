package ptest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Parent with a uint64 primary key, children referencing it through int64 and
// pointer foreign keys, using both Go field names and column names in tags.
type AFAuthor struct {
	psql.Name  `sql:"test_af_author"`
	ID         uint64       `sql:",key=PRIMARY"`
	AuthorName string       `sql:",type=VARCHAR,size=128"`
	DeletedAt  *time.Time   // enables soft delete on authors
	Books      []*AFBook    `psql:"has_many:AuthorID"`                     // Go field name
	BooksByCol []*AFBook    `psql:"has_many:author_id;order='Title DESC'"` // column name + order
	ValueBooks []AFBook     `psql:"has_many:AuthorID;order='Title ASC'"`   // value-typed slice
	Profile    *AFProfile   `psql:"has_one:author_id"`
	ValProfile AFProfile    `psql:"has_one:AuthorID"`
	Tags       []*AFTag     `psql:"many_to_many:test_af_author_tag,author_id,tag_id;order='Label DESC'"`
	ValueTags  []AFTag      `psql:"many_to_many:test_af_author_tag,author_id,tag_id"`
	Nothing    []*AFProfile `psql:"has_many:NoSuchField"`
}

type AFBook struct {
	psql.Name `sql:"test_af_book"`
	ID        int64      `sql:",key=PRIMARY"`
	AuthorID  *int64     `sql:"author_id,type=BIGINT,null=1"`
	Title     string     `sql:",type=VARCHAR,size=128"`
	DeletedAt *time.Time // soft delete on books
	Author    *AFAuthor  `psql:"belongs_to:AuthorID"`
	ValAuthor AFAuthor   `psql:"belongs_to:author_id"`
}

type AFProfile struct {
	psql.Name `sql:"test_af_profile"`
	ID        int64  `sql:",key=PRIMARY"`
	AuthorID  int64  `sql:"author_id,type=BIGINT"`
	Bio       string `sql:",type=VARCHAR,size=128"`
}

type AFTag struct {
	psql.Name `sql:"test_af_tag"`
	ID        []byte `sql:",type=VARBINARY,size=16,key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=64"`
}

type AFAuthorTag struct {
	psql.Name `sql:"test_af_author_tag"`
	AuthorID  int64  `sql:"author_id,type=BIGINT,key=PRIMARY"`
	TagID     []byte `sql:"tag_id,type=VARBINARY,size=16,key=PRIMARY"`
}

func afTables() []string {
	return []string{"test_af_author_tag", "test_af_tag", "test_af_profile", "test_af_book", "test_af_author"}
}

func setupAFTables(t *testing.T) context.Context {
	t.Helper()
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	drop := func() {
		for _, name := range afTables() {
			_ = psql.Q(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, name)).Exec(ctx)
		}
	}
	drop()
	t.Cleanup(drop)

	_ = psql.Table[AFAuthor]()
	_ = psql.Table[AFBook]()
	_ = psql.Table[AFProfile]()
	_ = psql.Table[AFTag]()
	_ = psql.Table[AFAuthorTag]()
	return ctx
}

func i64p(v int64) *int64 { return &v }

func afSeed(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: 1, AuthorName: "Alice"}))
	require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: 2, AuthorName: "Bob"}))
	require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: 3, AuthorName: "Carol"}))
	require.NoError(t, psql.Insert(ctx, &AFBook{ID: 1, AuthorID: i64p(1), Title: "A1"}))
	require.NoError(t, psql.Insert(ctx, &AFBook{ID: 2, AuthorID: i64p(1), Title: "A2"}))
	require.NoError(t, psql.Insert(ctx, &AFBook{ID: 3, AuthorID: i64p(2), Title: "B1"}))
	require.NoError(t, psql.Insert(ctx, &AFBook{ID: 4, AuthorID: nil, Title: "Orphan"}))
	require.NoError(t, psql.Insert(ctx, &AFProfile{ID: 1, AuthorID: 1, Bio: "alice bio"}))
	require.NoError(t, psql.Insert(ctx, &AFProfile{ID: 2, AuthorID: 3, Bio: "carol bio"}))
}

func TestAssocFixesPointerAndMismatchedFK(t *testing.T) {
	ctx := setupAFTables(t)
	afSeed(t, ctx)

	// belongs_to through a *int64 FK to a uint64 PK, both pointer and value fields
	books, err := psql.Fetch[AFBook](ctx, nil, psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, books, 4)
	require.NoError(t, psql.Preload(ctx, books, "Author", "ValAuthor"))
	assert.Equal(t, "Alice", books[0].Author.AuthorName)
	assert.Equal(t, "Alice", books[1].Author.AuthorName)
	assert.Equal(t, "Bob", books[2].Author.AuthorName)
	assert.Nil(t, books[3].Author, "nil FK leaves the association unset")
	assert.Equal(t, "Alice", books[0].ValAuthor.AuthorName)
	assert.Equal(t, "Bob", books[2].ValAuthor.AuthorName)
	assert.Equal(t, uint64(0), books[3].ValAuthor.ID)
	assert.Same(t, books[0].Author, books[1].Author, "parents referencing the same row share one pointer")

	// has_many / has_one from a uint64 PK to int64 / *int64 FKs
	authors, err := psql.Fetch[AFAuthor](ctx, nil, psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, authors, 3)
	require.NoError(t, psql.Preload(ctx, authors, "Books", "BooksByCol", "ValueBooks", "Profile", "ValProfile"))

	alice, bob, carol := authors[0], authors[1], authors[2]
	require.Len(t, alice.Books, 2)
	require.Len(t, alice.BooksByCol, 2)
	assert.Equal(t, "A2", alice.BooksByCol[0].Title, "order='Title DESC' honoured")
	assert.Equal(t, "A1", alice.BooksByCol[1].Title)
	require.Len(t, alice.ValueBooks, 2)
	assert.Equal(t, "A1", alice.ValueBooks[0].Title, "order='Title ASC' honoured on value slices")
	assert.Equal(t, "A2", alice.ValueBooks[1].Title)
	require.Len(t, bob.Books, 1)
	assert.Equal(t, "B1", bob.Books[0].Title)
	assert.Nil(t, carol.Books)

	require.NotNil(t, alice.Profile)
	assert.Equal(t, "alice bio", alice.Profile.Bio)
	assert.Equal(t, "alice bio", alice.ValProfile.Bio)
	assert.Nil(t, bob.Profile)
	assert.Equal(t, int64(0), bob.ValProfile.ID)
	assert.Equal(t, "carol bio", carol.ValProfile.Bio)

	// Unknown FK name is a descriptive error, not a panic.
	err = psql.Preload(ctx, authors, "Nothing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchField")
}

func TestAssocFixesChunking(t *testing.T) {
	ctx := setupAFTables(t)
	saved := psql.PreloadChunkSize
	psql.PreloadChunkSize = 2
	t.Cleanup(func() { psql.PreloadChunkSize = saved })

	const n = 7
	tags := make([]*AFTag, n)
	for i := 1; i <= n; i++ {
		require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: uint64(i), AuthorName: fmt.Sprintf("Author %d", i)}))
		for j := 0; j < 2; j++ {
			require.NoError(t, psql.Insert(ctx, &AFBook{ID: int64(i*10 + j), AuthorID: i64p(int64(i)), Title: fmt.Sprintf("Book %d-%d", i, j)}))
		}
		tags[i-1] = &AFTag{ID: []byte{byte(i), 0xff}, Label: fmt.Sprintf("tag%d", i)}
		require.NoError(t, psql.Insert(ctx, tags[i-1]))
		// every author gets its own tag plus tag 1
		require.NoError(t, psql.Insert(ctx, &AFAuthorTag{AuthorID: int64(i), TagID: tags[i-1].ID}))
		if i > 1 {
			require.NoError(t, psql.Insert(ctx, &AFAuthorTag{AuthorID: int64(i), TagID: tags[0].ID}))
		}
	}

	authors, err := psql.Fetch[AFAuthor](ctx, nil, psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, authors, n)
	require.NoError(t, psql.Preload(ctx, authors, "Books", "Tags", "ValueTags"))
	for i, a := range authors {
		require.Len(t, a.Books, 2, "author %d", a.ID)
		for _, b := range a.Books {
			assert.Equal(t, int64(a.ID), *b.AuthorID)
		}
		if i == 0 {
			require.Len(t, a.Tags, 1)
			assert.Equal(t, "tag1", a.Tags[0].Label)
		} else {
			require.Len(t, a.Tags, 2, "author %d", a.ID)
			// order='Label DESC': tagN before tag1
			assert.Equal(t, fmt.Sprintf("tag%d", i+1), a.Tags[0].Label)
			assert.Equal(t, "tag1", a.Tags[1].Label)
			assert.Len(t, a.ValueTags, 2)
		}
	}
	// many_to_many shares one pointer per target across parents
	assert.Same(t, authors[1].Tags[1], authors[2].Tags[1])

	books, err := psql.Fetch[AFBook](ctx, nil)
	require.NoError(t, err)
	require.Len(t, books, 2*n)
	require.NoError(t, psql.Preload(ctx, books, "Author"))
	for _, b := range books {
		require.NotNil(t, b.Author, "book %d", b.ID)
		assert.Equal(t, uint64(*b.AuthorID), b.Author.ID)
	}
}

func TestAssocFixesManyToManyBytesKeys(t *testing.T) {
	ctx := setupAFTables(t)
	require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: 1, AuthorName: "Alice"}))
	require.NoError(t, psql.Insert(ctx, &AFAuthor{ID: 2, AuthorName: "Bob"}))
	k1 := []byte("\x00\x01binary\xff")
	k2 := []byte("\x00\x02binary\xfe")
	require.NoError(t, psql.Insert(ctx, &AFTag{ID: k1, Label: "one"}))
	require.NoError(t, psql.Insert(ctx, &AFTag{ID: k2, Label: "two"}))
	require.NoError(t, psql.Insert(ctx, &AFAuthorTag{AuthorID: 1, TagID: k1}))
	require.NoError(t, psql.Insert(ctx, &AFAuthorTag{AuthorID: 1, TagID: k2}))
	require.NoError(t, psql.Insert(ctx, &AFAuthorTag{AuthorID: 2, TagID: k2}))

	authors, err := psql.Fetch[AFAuthor](ctx, nil, psql.Sort(psql.S("ID", "ASC")), psql.WithPreload("Tags", "ValueTags"))
	require.NoError(t, err)
	require.Len(t, authors, 2)
	require.Len(t, authors[0].Tags, 2)
	assert.Equal(t, "two", authors[0].Tags[0].Label, "order='Label DESC'")
	assert.Equal(t, "one", authors[0].Tags[1].Label)
	require.Len(t, authors[1].Tags, 1)
	assert.Equal(t, k2, authors[1].Tags[0].ID)
	require.Len(t, authors[0].ValueTags, 2)
	assert.Equal(t, "two", authors[1].ValueTags[0].Label)
}

func TestAssocFixesSoftDeletedChildren(t *testing.T) {
	ctx := setupAFTables(t)
	afSeed(t, ctx)
	// soft delete one of Alice's books and author Bob
	_, err := psql.Delete[AFBook](ctx, map[string]any{"ID": int64(2)})
	require.NoError(t, err)
	_, err = psql.Delete[AFAuthor](ctx, map[string]any{"ID": uint64(2)})
	require.NoError(t, err)

	authors, err := psql.Fetch[AFAuthor](ctx, nil, psql.IncludeDeleted(), psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, authors, 3)

	require.NoError(t, psql.Preload(ctx, authors, "Books"))
	assert.Len(t, authors[0].Books, 1, "soft-deleted child excluded by default")

	require.NoError(t, psql.PreloadOpts(ctx, authors, psql.IncludeDeleted(), "Books"))
	assert.Len(t, authors[0].Books, 2, "IncludeDeleted threads through to children")

	books, err := psql.Fetch[AFBook](ctx, nil, psql.IncludeDeleted(), psql.Sort(psql.S("ID", "ASC")))
	require.NoError(t, err)
	require.Len(t, books, 4)
	require.NoError(t, psql.Preload(ctx, books, "Author"))
	assert.Nil(t, books[2].Author, "soft-deleted parent excluded by default")
	require.NoError(t, psql.PreloadOpts(ctx, books, psql.IncludeDeleted(), "Author"))
	require.NotNil(t, books[2].Author)
	assert.Equal(t, "Bob", books[2].Author.AuthorName)

	// opt.Preload is used when no field list is given
	fresh, err := psql.Fetch[AFAuthor](ctx, map[string]any{"ID": uint64(1)})
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.NoError(t, psql.PreloadOpts(ctx, fresh, &psql.FetchOptions{Preload: []string{"Books"}, WithDeleted: true}))
	assert.Len(t, fresh[0].Books, 2)
}

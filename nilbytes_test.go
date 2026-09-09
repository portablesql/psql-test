package ptest

import (
	"context"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/require"
)

type nbItem struct {
	psql.Name `sql:"nb_item"`
	ID        int64  `sql:",key=PRIMARY"`
	Blob      []byte // NOT NULL by default
	Opt       []byte `sql:",null=1"`
}

// A nil []byte on the default NOT NULL column is stored as empty bytes, while
// a column declared nullable keeps NULL.
func TestNilBytesNotNullColumn(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "nb_item"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "nb_item"`).Exec(ctx) }()

	require.NoError(t, psql.Insert(ctx, &nbItem{ID: 1}))
	got, err := psql.Get[nbItem](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	require.Len(t, got.Blob, 0)
	require.Nil(t, got.Opt)
}

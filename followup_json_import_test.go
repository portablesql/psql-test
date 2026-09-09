package ptest

import (
	"context"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/require"
)

type fbJSONDoc struct {
	psql.Name `sql:"fb_json_doc"`
	ID        uint64         `sql:",key=PRIMARY"`
	Meta      map[string]any `sql:",import=JSON"`
}

// import=JSON (an engine magic type carrying format=json) must select the
// JSON setter instead of panicking on the map type.
func TestFollowupImportJSON(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "fb_json_doc"`).Exec(ctx)
	defer func() { _ = psql.Q(`DROP TABLE IF EXISTS "fb_json_doc"`).Exec(ctx) }()

	require.NoError(t, psql.Insert(ctx, &fbJSONDoc{ID: 1, Meta: map[string]any{"a": "b", "n": float64(2)}}))
	got, err := psql.Get[fbJSONDoc](ctx, map[string]any{"ID": 1})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": "b", "n": float64(2)}, got.Meta)
}

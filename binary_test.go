package ptest

import (
	"bytes"
	"context"
	"testing"

	"github.com/portablesql/psql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === Binary data ([]byte) integration tests ===

type BinaryTable struct {
	psql.Name `sql:"test_binary"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=128"`
	Data      []byte
}

func TestBinaryInsertAndRead(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	data := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0xff}
	err := psql.Insert(ctx, &BinaryTable{ID: 1, Label: "test", Data: data})
	require.NoError(t, err)

	obj, err := psql.Get[BinaryTable](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "test", obj.Label)
	assert.True(t, bytes.Equal(data, obj.Data), "expected %x, got %x", data, obj.Data)

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

func TestBinaryUpdate(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	err := psql.Insert(ctx, &BinaryTable{ID: 1, Label: "original", Data: []byte{0x01, 0x02}})
	require.NoError(t, err)

	// Update binary data via builder
	newData := []byte{0xca, 0xfe, 0xba, 0xbe}
	_, err = psql.B().Update("test_binary").
		Set(map[string]any{"Data": newData}).
		Where(map[string]any{"ID": int64(1)}).
		ExecQuery(ctx)
	require.NoError(t, err)

	obj, err := psql.Get[BinaryTable](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.True(t, bytes.Equal(newData, obj.Data), "expected %x, got %x", newData, obj.Data)

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

func TestBinaryEmpty(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	err := psql.Insert(ctx, &BinaryTable{ID: 1, Label: "empty", Data: []byte{}})
	require.NoError(t, err)

	obj, err := psql.Get[BinaryTable](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Len(t, obj.Data, 0) // may be nil or empty depending on driver

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

func TestBinaryLargePayload(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	// 32KB of binary data (must fit in MySQL BLOB which is 65535 bytes max)
	data := make([]byte, 32*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := psql.Insert(ctx, &BinaryTable{ID: 1, Label: "large", Data: data})
	require.NoError(t, err)

	obj, err := psql.Get[BinaryTable](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, obj.Data), "large binary data roundtrip failed (len: expected %d, got %d)", len(data), len(obj.Data))

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

func TestBinaryWhereEqual(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	d1 := []byte{0xaa, 0xbb}
	d2 := []byte{0xcc, 0xdd}
	require.NoError(t, psql.Insert(ctx, &BinaryTable{ID: 1, Label: "first", Data: d1}))
	require.NoError(t, psql.Insert(ctx, &BinaryTable{ID: 2, Label: "second", Data: d2}))

	// Find by binary data
	results, err := psql.Fetch[BinaryTable](ctx, map[string]any{"Data": d1})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	if len(results) > 0 {
		assert.Equal(t, "first", results[0].Label)
	}

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

type BinaryNullableTable struct {
	psql.Name `sql:"test_binary_nullable"`
	ID        int64  `sql:",key=PRIMARY"`
	Label     string `sql:",type=VARCHAR,size=128"`
	Data      []byte `sql:",import=[]uint8,null=1"`
}

func TestBinaryNilInsert(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary_nullable"`).Exec(ctx)

	// Insert with nil []byte — should store as SQL NULL, not cause encoding errors.
	err := psql.Insert(ctx, &BinaryNullableTable{ID: 1, Label: "nil-data"})
	require.NoError(t, err)

	obj, err := psql.Get[BinaryNullableTable](ctx, map[string]any{"ID": int64(1)})
	require.NoError(t, err)
	assert.Equal(t, "nil-data", obj.Label)
	assert.Nil(t, obj.Data, "nil []byte should roundtrip as nil")

	// Also insert a row with non-nil data to confirm both work in the same table
	err = psql.Insert(ctx, &BinaryNullableTable{ID: 2, Label: "has-data", Data: []byte{0xab, 0xcd}})
	require.NoError(t, err)

	obj2, err := psql.Get[BinaryNullableTable](ctx, map[string]any{"ID": int64(2)})
	require.NoError(t, err)
	assert.True(t, bytes.Equal([]byte{0xab, 0xcd}, obj2.Data))

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary_nullable"`).Exec(ctx)
}

func TestBinaryMultipleRows(t *testing.T) {
	be := getTestBackend(t)
	ctx := be.Plug(context.Background())
	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)

	rows := []BinaryTable{
		{ID: 1, Label: "one", Data: []byte{0x01}},
		{ID: 2, Label: "two", Data: []byte{0x02, 0x03}},
		{ID: 3, Label: "three", Data: []byte{0x04, 0x05, 0x06}},
	}
	for _, r := range rows {
		require.NoError(t, psql.Insert(ctx, &r))
	}

	results, err := psql.Fetch[BinaryTable](ctx, nil)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	// Verify each row's data is independent (not sharing buffer)
	for i, r := range results {
		assert.True(t, bytes.Equal(rows[i].Data, r.Data),
			"row %d: expected %x, got %x", r.ID, rows[i].Data, r.Data)
	}

	_ = psql.Q(`DROP TABLE IF EXISTS "test_binary"`).Exec(ctx)
}

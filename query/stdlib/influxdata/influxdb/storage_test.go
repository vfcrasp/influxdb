package influxdb_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func TestStorage_Transformation(t *testing.T) {
	testCases := []struct {
		name    string
		tx      func(d execute.Dataset, c execute.TableBuilderCache) (execute.Transformation, error)
		data    []*executetest.Table
		want    []*executetest.Table
		wantErr bool
	}{
		{
			name: "limit",
			tx: func(d execute.Dataset, c execute.TableBuilderCache) (execute.Transformation, error) {
				return universe.NewLimitTransformation(d, c, &universe.LimitProcedureSpec{N: 4}), nil
			},
			data: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(10), execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 4.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 5.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 6.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 7.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 8.0, "m1", "f2"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(10), execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 4.0, "m1", "f2"},
					},
				},
			},
		},
		{
			name: "group",
			tx: func(d execute.Dataset, c execute.TableBuilderCache) (execute.Transformation, error) {
				return universe.NewGroupTransformation(d, c, &universe.GroupProcedureSpec{
					GroupMode: flux.GroupModeBy,
					GroupKeys: []string{"_measurement", "_field"},
				}), nil
			},
			data: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(10), execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 4.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 5.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 6.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 7.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 8.0, "m1", "f2"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_measurement", "_field"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(10), execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 5.0, "m1", "f1"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 7.0, "m1", "f1"},
					},
				},
				{
					KeyCols: []string{"_measurement", "_field"},
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_value", Type: flux.TFloat},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(10), execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(2), 4.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(11), 6.0, "m1", "f2"},
						{execute.Time(0), execute.Time(10), execute.Time(12), 8.0, "m1", "f2"},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			bounds := execute.Bounds{
				Start: execute.Time(0),
				Stop:  execute.Time(100),
			}
			now := execute.Time(0)

			store, err := newMockStore(tc.data)
			if err != nil {
				t.Fatal(err)
			}

			id := executetest.RandomDatasetID()
			d := executetest.NewDataset(id)
			c := execute.NewTableBuilderCache(executetest.UnlimitedAllocator)
			c.SetTriggerSpec(flux.DefaultTrigger)
			s := influxdb.NewSource(id, reads.NewReader(store), influxdb.ReadSpec{PointsLimit: 1000}, bounds,
				execute.Window{Every: execute.Duration(values.Duration(time.Millisecond))}, now)

			tx, err := tc.tx(d, c)
			if err != nil {
				t.Fatal(err)
			}

			s.AddTransformation(tx)
			// TODO(affo) how to extract a possible error during transformation execution?
			s.Run(context.Background())

			got, err := executetest.TablesFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeTables(got)
			executetest.NormalizeTables(tc.want)

			sort.Sort(executetest.SortedTables(got))
			sort.Sort(executetest.SortedTables(tc.want))

			// TODO(affo) will compare the array of tables; for now, only data
			dWant := make([][][]interface{}, len(tc.want))
			for _, tbl := range tc.want {
				dWant = append(dWant, tbl.Data)
			}

			dGot := make([][][]interface{}, len(got))
			for _, tbl := range got {
				dGot = append(dGot, tbl.Data)
			}

			if !cmp.Equal(dWant, dGot) {
				t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(dWant, dGot))
			}
		})
	}
}

// ----- Store

type mockStore struct {
	cur          reads.SeriesCursor
	tIdxs, mIdxs []int
}

func newMockStore(data []*executetest.Table) (*mockStore, error) {
	ms := &mockStore{}
	if err := ms.checkTables(data); err != nil {
		return nil, err
	}
	ms.cur = &tableCursor{data: data, mIdxs: ms.mIdxs, tIdxs: ms.tIdxs}
	return ms, nil
}

func (ms *mockStore) checkTables(tables []*executetest.Table) error {
	ms.mIdxs = make([]int, 0, len(tables))
	for _, tbl := range tables {
		mIdx := -1
		tIdx := -1
		for i, c := range tbl.ColMeta {
			if c.Label == "_measurement" && c.Type == flux.TString {
				mIdx = i
			} else if c.Label == "_time" && c.Type == flux.TTime {
				tIdx = i
			}
		}

		if mIdx < 0 || tIdx < 0 {
			return fmt.Errorf(`every input table must have "_measurement" and "_time" column`)
		}

		ms.mIdxs = append(ms.mIdxs, mIdx)
		ms.tIdxs = append(ms.tIdxs, tIdx)
	}

	return nil
}

func (ms *mockStore) Read(ctx context.Context, req *datatypes.ReadRequest) (reads.ResultSet, error) {
	return reads.NewResultSet(ctx, req, ms.cur), nil
}

func (*mockStore) GroupRead(ctx context.Context, req *datatypes.ReadRequest) (reads.GroupResultSet, error) {
	//TODO(affo) implement
	return nil, nil
}

func (*mockStore) GetSource(rs influxdb.ReadSpec) (proto.Message, error) {
	return &mockMsg{}, nil
}

type mockMsg struct{}

func (m *mockMsg) Reset() {
	*m = mockMsg{}
}

func (*mockMsg) String() string {
	return "mockMsg()"
}

func (*mockMsg) ProtoMessage() {
}

// ----- Cursors

type tableCursor struct {
	data         []*executetest.Table
	tIdxs, mIdxs []int
	i, ii        int
}

func (tc *tableCursor) Close()     {}
func (tc *tableCursor) Err() error { return nil }

func (tc *tableCursor) Next() *reads.SeriesRow {
	if tc.i >= len(tc.data) {
		return nil
	}

	tbl := tc.data[tc.i]
	if tc.ii >= len(tbl.Data) {
		tc.i++
		return tc.Next()
	}

	row := tbl.Data[tc.ii]
	sr := &reads.SeriesRow{}
	sr.Name = []byte(row[tc.mIdxs[tc.i]].(string))

	gk := tbl.KeyCols
	gi := 0
	for j, c := range tbl.ColMeta {
		if gi < len(gk) && gk[gi] == c.Label {
			tag := models.Tag{
				Key:   []byte(c.Label),
				Value: []byte(valueToString(c, tbl, tc.ii, j)),
			}
			sr.Tags = append(sr.Tags, tag)
			sr.SeriesTags = append(sr.SeriesTags, tag.Clone())
			gi++
		}

		itr := &cursorIterator{}
		switch c.Type {
		case flux.TBool:
			itr.c = &boolCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		case flux.TInt:
			itr.c = &intCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		case flux.TUInt:
			itr.c = &uintCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		case flux.TFloat:
			itr.c = &floatCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		case flux.TString:
			itr.c = &stringCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		case flux.TTime:
			itr.c = &intCursor{data: tbl, timeIdx: tc.tIdxs[tc.i], colIdx: j}
		}

		sr.Query = append(sr.Query, itr)
	}

	tc.ii++
	return sr
}

func valueToString(c flux.ColMeta, tbl *executetest.Table, i int, j int) string {
	switch c.Type {
	case flux.TBool:
		return strconv.FormatBool(tbl.Data[i][j].(bool))
	case flux.TInt:
		return strconv.FormatInt(tbl.Data[i][j].(int64), 10)
	case flux.TUInt:
		return strconv.FormatUint(tbl.Data[i][j].(uint64), 10)
	case flux.TFloat:
		return strconv.FormatFloat(tbl.Data[i][j].(float64), 'f', 64, -1)
	case flux.TString:
		return tbl.Data[i][j].(string)
	case flux.TTime:
		t := tbl.Data[i][j].(values.Time)
		return strconv.FormatInt(int64(t), 10)
	}

	return ""
}

type uselessStats struct{}

func (uselessStats) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}

type baseCursor struct{}

func (baseCursor) Close() {}

func (baseCursor) Err() error {
	return nil
}

type cursorIterator struct {
	uselessStats
	c    cursors.Cursor
	stop bool
}

func (ci *cursorIterator) Next(ctx context.Context, r *cursors.CursorRequest) (cursors.Cursor, error) {
	if ci.stop {
		return nil, nil
	}

	ci.stop = true
	return ci.c, nil
}

func extractInts(data *executetest.Table, j int) []int64 {
	s := make([]int64, 0, len(data.Data))
	for _, row := range data.Data {
		var v int64
		if ts, ok := row[j].(values.Time); ok {
			v = int64(ts)
		} else {
			v = row[j].(int64)
		}
		s = append(s, v)
	}
	return s
}

func extractUInts(data *executetest.Table, j int) []uint64 {
	s := make([]uint64, 0, len(data.Data))
	for _, row := range data.Data {
		s = append(s, row[j].(uint64))
	}
	return s
}

func extractFloats(data *executetest.Table, j int) []float64 {
	s := make([]float64, 0, len(data.Data))
	for _, row := range data.Data {
		s = append(s, row[j].(float64))
	}
	return s
}

func extractBools(data *executetest.Table, j int) []bool {
	s := make([]bool, 0, len(data.Data))
	for _, row := range data.Data {
		s = append(s, row[j].(bool))
	}
	return s
}

func extractStrings(data *executetest.Table, j int) []string {
	s := make([]string, 0, len(data.Data))
	for _, row := range data.Data {
		s = append(s, row[j].(string))
	}
	return s
}

type intCursor struct {
	uselessStats
	baseCursor
	data            *executetest.Table
	timeIdx, colIdx int
}

func (c *intCursor) Next() *cursors.IntegerArray {
	return &cursors.IntegerArray{
		Timestamps: extractInts(c.data, c.timeIdx),
		Values:     extractInts(c.data, c.colIdx),
	}
}

type uintCursor struct {
	uselessStats
	baseCursor
	data            *executetest.Table
	timeIdx, colIdx int
}

func (c *uintCursor) Next() *cursors.UnsignedArray {
	return &cursors.UnsignedArray{
		Timestamps: extractInts(c.data, c.timeIdx),
		Values:     extractUInts(c.data, c.colIdx),
	}
}

type floatCursor struct {
	uselessStats
	baseCursor
	data            *executetest.Table
	timeIdx, colIdx int
}

func (c *floatCursor) Next() *cursors.FloatArray {
	return &cursors.FloatArray{
		Timestamps: extractInts(c.data, c.timeIdx),
		Values:     extractFloats(c.data, c.colIdx),
	}
}

type boolCursor struct {
	uselessStats
	baseCursor
	data            *executetest.Table
	timeIdx, colIdx int
}

func (c *boolCursor) Next() *cursors.BooleanArray {
	return &cursors.BooleanArray{
		Timestamps: extractInts(c.data, c.timeIdx),
		Values:     extractBools(c.data, c.colIdx),
	}
}

type stringCursor struct {
	uselessStats
	baseCursor
	data            *executetest.Table
	timeIdx, colIdx int
}

func (c *stringCursor) Next() *cursors.StringArray {
	return &cursors.StringArray{
		Timestamps: extractInts(c.data, c.timeIdx),
		Values:     extractStrings(c.data, c.colIdx),
	}
}

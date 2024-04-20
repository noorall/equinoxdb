package equinox

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkIteratorResult(t *testing.T, table *Table, n int, reverse bool) {
	iter := table.NewIterator(reverse)
	defer iter.Close()

	count := 0
	if reverse {
		count = n - 1
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := KeyWithVersion([]byte(key("key", count)), 0)
		v := fmt.Sprintf("%d", count)

		require.EqualValues(t, k, iter.Key())
		require.EqualValues(t, v, string(iter.Value().Value))
		if reverse {
			count--
		} else {
			count++
		}
	}

	require.False(t, iter.Valid())
	expected := n
	if reverse {
		expected = -1
	}

	require.Equal(t, expected, count)
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		opts := getTestTableOptions()
		table := buildTestTable(t, "key", n, opts)
		defer table.DecrRef()

		t.Run(fmt.Sprintf("forword n=%d", n), func(t *testing.T) {
			checkIteratorResult(t, table, n, false)
		})

		t.Run(fmt.Sprintf("reverse n=%d", n), func(t *testing.T) {
			checkIteratorResult(t, table, n, true)
		})
	}
}

type seekCase struct {
	in    string
	valid bool
	out   string
	oval  string
}

func TestTableIteratorSeek(t *testing.T) {
	opts := getTestTableOptions()
	table := buildTestTable(t, "k", 10000, opts)
	defer table.DecrRef()
	t.Run("forward seek", func(t *testing.T) {
		iter := table.NewIterator(false)
		defer iter.Close()

		var cases = []seekCase{
			{"abc", true, "k0000", "0"},
			{"k0100", true, "k0100", "100"},
			{"k0100b", true, "k0101", "101"}, // Test case where we jump to next block.
			{"k1234", true, "k1234", "1234"},
			{"k1234b", true, "k1235", "1235"},
			{"k9999", true, "k9999", "9999"},
			{"z", false, "", ""},
		}

		for _, cas := range cases {
			iter.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, iter.Valid())
				continue
			}

			require.True(t, iter.Valid())
			k := ParseKey(iter.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, iter.Value().Value)
		}
	})

	t.Run("reverse seek", func(t *testing.T) {
		iter := table.NewIterator(true)
		defer iter.Close()

		var cases = []seekCase{
			{"abc", false, "", ""},
			{"k0100", true, "k0100", "100"},
			{"k0101b", true, "k0101", "101"}, // Test case where we jump to next block.
			{"k1234", true, "k1234", "1234"},
			{"k1234b", true, "k1234", "1234"},
			{"k9999", true, "k9999", "9999"},
			{"z", true, "k9999", "9999"},
		}

		for _, cas := range cases {
			iter.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, iter.Valid())
				continue
			}

			require.True(t, iter.Valid())
			k := ParseKey(iter.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, iter.Value().Value)
		}
	})
}

func TestUniIterator(t *testing.T) {
	opts := getTestTableOptions()
	table := buildTestTable(t, "key", 10000, opts)
	defer table.DecrRef()
	{
		iter := table.NewIterator(false)
		defer iter.Close()
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			k := KeyWithVersion([]byte(key("key", count)), 0)
			v := fmt.Sprintf("%d", count)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count++
		}
		require.EqualValues(t, 10000, count)
	}
	{
		iter := table.NewIterator(true)
		defer iter.Close()
		count := 10000
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			k := KeyWithVersion([]byte(key("key", count-1)), 0)
			v := fmt.Sprintf("%d", count-1)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count--
		}
		require.EqualValues(t, 0, count)
	}
}

func TestTablesIteratorOneTable(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	}, opts)

	defer tbl.DecrRef()

	iter := NewTablesIterator([]*Table{tbl}, false)
	defer iter.Close()

	iter.SeekToFirst()
	require.True(t, iter.Valid())

	require.EqualValues(t, "k1", string(ParseKey(iter.currIter.Key())))
	require.EqualValues(t, "a1", string(iter.Value().Value))
}

func TestTablesIterator(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "keya", 10000, opts)
	tbl2 := buildTestTable(t, "keyb", 10000, opts)
	tbl3 := buildTestTable(t, "keyc", 10000, opts)
	defer tbl.DecrRef()
	defer tbl2.DecrRef()
	defer tbl3.DecrRef()

	t.Run("forword", func(t *testing.T) {
		iter := NewTablesIterator([]*Table{tbl, tbl2, tbl3}, false)
		defer iter.Close()

		count := 0
		prefix := "key`"
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if count%10000 == 0 {
				bs := []byte(prefix)
				bs[3] += 1
				prefix = string(bs)
			}
			k := KeyWithVersion([]byte(key(prefix, count%10000)), 0)
			v := fmt.Sprintf("%d", count%10000)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count++
		}
		require.EqualValues(t, 30000, count)

		var cases = []seekCase{
			{"a", true, "keya0000", "0"},
			{"keyb", true, "keyb0000", "0"},
			{"keyb9999b", true, "keyc0000", "0"}, // Test case where we jump to next table.
			{"keyd", false, "", ""},
		}

		for _, cas := range cases {
			iter.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, iter.Valid())
				continue
			}

			require.True(t, iter.Valid())
			k := ParseKey(iter.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, iter.Value().Value)
		}
	})

	t.Run("reverse", func(t *testing.T) {
		iter := NewTablesIterator([]*Table{tbl, tbl2, tbl3}, true)
		defer iter.Close()

		count := 30000
		prefix := "keyd"
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if count%10000 == 0 {
				bs := []byte(prefix)
				bs[3] -= 1
				prefix = string(bs)
			}
			k := KeyWithVersion([]byte(key(prefix, (count-1)%10000)), 0)
			v := fmt.Sprintf("%d", (count-1)%10000)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count--
		}
		require.EqualValues(t, 0, count)

		var cases = []seekCase{
			{"a", false, "", ""},
			{"keyb", true, "keya9999", "9999"}, // Test case where we jump to next table.
			{"keyb9999b", true, "keyb9999", "9999"},
			{"keyd", true, "keyc9999", "9999"},
		}

		for _, cas := range cases {
			iter.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, iter.Valid())
				continue
			}

			require.True(t, iter.Valid())
			k := ParseKey(iter.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, iter.Value().Value)
		}
	})
}

type expectedResult struct {
	key   string
	value string
}

func checkMergeResult(t *testing.T, opts *Options, t1, t2 *Table, expected []expectedResult,
	seekKey []byte, reverse bool) {
	it1 := t1.NewIterator(reverse)
	it2 := NewTablesIterator([]*Table{t2}, reverse)
	it := NewTablesMergeIterator(opts, []Iterator{it1, it2}, reverse)
	defer it.Close()

	count := 0
	if seekKey == nil {
		it.SeekToFirst()
	} else {
		it.Seek(seekKey)
	}
	for ; it.Valid(); it.Next() {
		k := it.Key()
		vs := it.Value()

		require.EqualValues(t, expected[count].key, string(ParseKey(k)))
		require.EqualValues(t, expected[count].value, string(vs.Value))
		count++
	}
	require.Equal(t, len(expected), count)
	require.False(t, it.Valid())
}

func TestTablesMergeIterator(t *testing.T) {
	opts := getTestTableOptions()
	t1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k4", "a4"},
		{"k5", "a5"},
	}, opts)
	t2 := buildTable(t, [][]string{
		{"k2", "b2"},
		{"k3", "b3"},
		{"k4", "b4"},
	}, opts)
	defer t1.DecrRef()
	defer t2.DecrRef()

	t.Run("forward", func(t *testing.T) {
		expected := []expectedResult{
			{"k1", "a1"},
			{"k2", "b2"},
			{"k3", "b3"},
			{"k4", "a4"},
			{"k5", "a5"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, false)
	})

	t.Run("reverse", func(t *testing.T) {
		expected := []expectedResult{
			{"k5", "a5"},
			{"k4", "a4"},
			{"k3", "b3"},
			{"k2", "b2"},
			{"k1", "a1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, true)
	})
}

func TestTablesMergeIteratorRightLess(t *testing.T) {
	opts := getTestTableOptions()
	t1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	}, opts)
	t2 := buildTable(t, [][]string{{"l1", "b1"}}, opts)
	defer t1.DecrRef()
	defer t2.DecrRef()

	t.Run("forward", func(t *testing.T) {
		expected := []expectedResult{
			{"k1", "a1"},
			{"k2", "a2"},
			{"l1", "b1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, false)
	})

	t.Run("reverse", func(t *testing.T) {
		expected := []expectedResult{
			{"l1", "b1"},
			{"k2", "a2"},
			{"k1", "a1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, true)
	})
}

func TestTablesMergeIteratorLeftLess(t *testing.T) {
	opts := getTestTableOptions()
	t2 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	}, opts)
	t1 := buildTable(t, [][]string{{"l1", "b1"}}, opts)
	defer t1.DecrRef()
	defer t2.DecrRef()

	t.Run("forward", func(t *testing.T) {
		expected := []expectedResult{
			{"k1", "a1"},
			{"k2", "a2"},
			{"l1", "b1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, false)
	})

	t.Run("reverse", func(t *testing.T) {
		expected := []expectedResult{
			{"l1", "b1"},
			{"k2", "a2"},
			{"k1", "a1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, nil, true)
	})
}

func TestTablesMergeIteratorSeek(t *testing.T) {
	opts := getTestTableOptions()
	t1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k4", "a4"},
		{"k5", "a5"},
	}, opts)
	t2 := buildTable(t, [][]string{
		{"k2", "b2"},
		{"k3", "b3"},
		{"k4", "b4"},
	}, opts)
	defer t1.DecrRef()
	defer t2.DecrRef()

	seekKey := KeyWithVersion([]byte("k3"), 0)

	t.Run("forward", func(t *testing.T) {
		expected := []expectedResult{
			{"k3", "b3"},
			{"k4", "a4"},
			{"k5", "a5"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, seekKey, false)
	})

	t.Run("reverse", func(t *testing.T) {
		expected := []expectedResult{
			{"k3", "b3"},
			{"k2", "b2"},
			{"k1", "a1"},
		}
		checkMergeResult(t, &opts, t1, t2, expected, seekKey, true)
	})
}

func TestTablesMergeIteratorNested(t *testing.T) {
	opts := getTestTableOptions()
	t1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k4", "a4"},
		{"k5", "a5"},
	}, opts)
	t2 := buildTable(t, [][]string{
		{"k2", "b2"},
		{"k3", "b3"},
		{"k4", "b4"},
	}, opts)
	t3 := buildTable(t, [][]string{
		{"k4", "b4"},
		{"k5", "a5"},
		{"k6", "b6"},
		{"k7", "b7"},
	}, opts)
	t4 := buildTable(t, [][]string{
		{"k8", "b8"},
		{"k9", "b9"},
	}, opts)
	defer t1.DecrRef()
	defer t2.DecrRef()
	defer t3.DecrRef()
	defer t4.DecrRef()

	t.Run("forward", func(t *testing.T) {
		iter1 := NewTablesMergeIterator(&opts, []Iterator{t1.NewIterator(false),
			t2.NewIterator(false)}, false)
		iter2 := NewTablesIterator([]*Table{t3}, false)
		iter3 := t4.NewIterator(false)
		it := NewTablesMergeIterator(&opts, []Iterator{iter1, iter2, iter3}, false)
		defer it.Close()

		expected := []expectedResult{
			{"k1", "a1"},
			{"k2", "b2"},
			{"k3", "b3"},
			{"k4", "a4"},
			{"k5", "a5"},
			{"k6", "b6"},
			{"k7", "b7"},
			{"k8", "b8"},
			{"k9", "b9"},
		}

		count := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			k := it.Key()
			vs := it.Value()

			require.EqualValues(t, expected[count].key, string(ParseKey(k)))
			require.EqualValues(t, expected[count].value, string(vs.Value))
			count++
		}
		require.Equal(t, len(expected), count)
		require.False(t, it.Valid())

		var cases = []seekCase{
			{"k0", true, "k1", "a1"},
			{"k4a", true, "k5", "a5"},
			{"k9", true, "k9", "b9"},
			{"k9a", false, "", ""},
		}

		for _, cas := range cases {
			it.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, it.Valid())
				continue
			}

			require.True(t, it.Valid())
			k := ParseKey(it.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, it.Value().Value)
		}

	})

	t.Run("reverse", func(t *testing.T) {
		iter1 := NewTablesMergeIterator(&opts, []Iterator{t1.NewIterator(true),
			t2.NewIterator(true)}, true)
		iter2 := NewTablesIterator([]*Table{t3}, true)
		iter3 := t4.NewIterator(true)
		it := NewTablesMergeIterator(&opts, []Iterator{iter1, iter2, iter3}, true)
		defer it.Close()

		expected := []expectedResult{
			{"k9", "b9"},
			{"k8", "b8"},
			{"k7", "b7"},
			{"k6", "b6"},
			{"k5", "a5"},
			{"k4", "a4"},
			{"k3", "b3"},
			{"k2", "b2"},
			{"k1", "a1"},
		}

		count := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			k := it.Key()
			vs := it.Value()

			require.EqualValues(t, expected[count].key, string(ParseKey(k)))
			require.EqualValues(t, expected[count].value, string(vs.Value))
			count++
		}
		require.Equal(t, len(expected), count)
		require.False(t, it.Valid())

		var cases = []seekCase{
			{"k0", false, "", ""},
			{"k4a", true, "k4", "a4"},
			{"k9", true, "k9", "b9"},
			{"k9a", true, "k9", "b9"},
		}

		for _, cas := range cases {
			it.Seek(KeyWithVersion([]byte(cas.in), 0))
			if !cas.valid {
				require.False(t, it.Valid())
				continue
			}

			require.True(t, it.Valid())
			k := ParseKey(it.Key())
			require.EqualValues(t, cas.out, string(k))
			require.EqualValues(t, cas.oval, it.Value().Value)
		}
	})

}

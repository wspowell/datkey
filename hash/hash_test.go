package hash_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wspowell/datkey/hash"
)

func Test_Slot(t *testing.T) {
	t.Parallel()

	// These should never change between runs.
	assert.Equal(t, hash.Slot(0x281), hash.ToSlot("test0"))
	assert.Equal(t, hash.Slot(0x12a0), hash.ToSlot("test1"))
	assert.Equal(t, hash.Slot(0x22c3), hash.ToSlot("test2"))
	assert.Equal(t, hash.Slot(0x32e2), hash.ToSlot("test3"))
	assert.Equal(t, hash.Slot(0x205), hash.ToSlot("test4"))
	assert.Equal(t, hash.Slot(0x1224), hash.ToSlot("test5"))
	assert.Equal(t, hash.Slot(0x2247), hash.ToSlot("test6"))
	assert.Equal(t, hash.Slot(0x3266), hash.ToSlot("test7"))
	assert.Equal(t, hash.Slot(0x389), hash.ToSlot("test8"))
	assert.Equal(t, hash.Slot(0x13a8), hash.ToSlot("test9"))
}

func Test_Range(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		slot             hash.Slot
		hashRange        hash.Range
		expectedContains bool
	}{
		{
			slot: hash.Slot(1),
			hashRange: hash.Range{
				Begin: hash.Slot(1),
				End:   hash.Slot(1),
			},
			expectedContains: true,
		},
		{
			slot: hash.Slot(0),
			hashRange: hash.Range{
				Begin: hash.Slot(1),
				End:   hash.Slot(1),
			},
			expectedContains: false,
		},
		{
			slot: hash.Slot(5),
			hashRange: hash.Range{
				Begin: hash.Slot(4),
				End:   hash.Slot(6),
			},
			expectedContains: true,
		},
		{
			slot: hash.Slot(0),
			hashRange: hash.Range{
				Begin: hash.Slot(0),
				End:   hash.Slot(16384),
			},
			expectedContains: true,
		},
		{
			slot: hash.Slot(16384),
			hashRange: hash.Range{
				Begin: hash.Slot(0),
				End:   hash.Slot(16384),
			},
			expectedContains: true,
		},
		{
			slot: hash.Slot(0),
			hashRange: hash.Range{
				Begin: hash.Slot(1),
				End:   hash.Slot(16383),
			},
			expectedContains: false,
		},
		{
			slot: hash.Slot(16384),
			hashRange: hash.Range{
				Begin: hash.Slot(0),
				End:   hash.Slot(16383),
			},
			expectedContains: false,
		},
	}
	for index := range testCases {
		testCase := testCases[index]
		t.Run(fmt.Sprintf("slot %d in range [%d, %d] = %t", testCase.slot, testCase.hashRange.Begin, testCase.hashRange.End, testCase.expectedContains), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.expectedContains, testCase.hashRange.Contains(testCase.slot))
		})
	}
}

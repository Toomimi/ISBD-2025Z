package tomy_file

import (
	"math"
	"testing"
)

func TestZigZagEncoding(t *testing.T) {
	tests := []struct {
		original int64
		expected uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-3, 5},
		{math.MaxInt32, 4294967294},
		{math.MinInt32, 4294967295},
		{math.MaxInt64, 0xFFFFFFFFFFFFFFFE},
		{math.MinInt64, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tc := range tests {
		encoded := ZigZagEncode(tc.original)
		if encoded != tc.expected {
			t.Errorf("ZigZagEncode(%d): expected %d, received %d", tc.original, tc.expected, encoded)
		}

		decoded := ZigZagDecode(encoded)
		if decoded != tc.original {
			t.Errorf("ZigZag Decode Failure: Input %d -> Encoded %d -> Decoded %d", tc.original, encoded, decoded)
		}
	}
}

func TestZigZagProperty(t *testing.T) {
	inputs := []int64{
		0, -1, 1, -100, 100,
		-1234567890, 1234567890,
		math.MinInt64, math.MaxInt64,
	}

	for _, input := range inputs {
		encoded := ZigZagEncode(input)
		decoded := ZigZagDecode(encoded)

		if input != decoded {
			t.Errorf("Expected: %d Received: %d", input, decoded)
		}
	}
}

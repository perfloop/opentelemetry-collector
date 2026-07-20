// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRecordSlicePrefixTransfer(t *testing.T) {
	expected := generateTestLogRecordSlice()
	source := generateTestLogRecordSlice()
	destination := NewLogRecordSlice()
	destination.AppendEmpty().SetSeverityText("existing")

	moveFirstLogRecords(source, 2, destination)

	require.Equal(t, expected.Len()-2, source.Len())
	require.Equal(t, 3, destination.Len())
	require.Equal(t, "existing", destination.At(0).SeverityText())
	require.Equal(t, expected.At(0), destination.At(1))
	require.Equal(t, expected.At(1), destination.At(2))
	require.Equal(t, expected.At(2), source.At(0))
	require.Equal(t, expected.At(3), source.At(1))
}

type logRecordPrefixMover interface {
	MoveFirstNTo(int, LogRecordSlice)
}

func moveFirstLogRecords(source LogRecordSlice, count int, destination LogRecordSlice) {
	if mover, ok := any(source).(logRecordPrefixMover); ok {
		mover.MoveFirstNTo(count, destination)
		return
	}

	remaining := count
	source.RemoveIf(func(record LogRecord) bool {
		if remaining == 0 {
			return false
		}
		record.MoveTo(destination.AppendEmpty())
		remaining--
		return true
	})
}

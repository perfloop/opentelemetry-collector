// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	source := NewLogRecordSlice()
	for _, severityText := range []string{"first", "second", "third", "fourth"} {
		source.AppendEmpty().SetSeverityText(severityText)
	}
	destination := NewLogRecordSlice()
	destination.AppendEmpty().SetSeverityText("existing")

	source.MoveFirstNTo(2, destination)
	source.MoveFirstNTo(1, destination)

	require.Equal(t, 1, source.Len())
	require.Equal(t, "fourth", source.At(0).SeverityText())
	require.Equal(t, []string{"existing", "first", "second", "third"}, severityTexts(destination))
}

func TestLogRecordSliceMoveFirstNToAliased(t *testing.T) {
	logs := NewLogs()
	source := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, severityText := range []string{"first", "second", "third"} {
		source.AppendEmpty().SetSeverityText(severityText)
	}

	destination := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	source.MoveFirstNTo(1, destination)

	require.Equal(t, []string{"first", "second", "third"}, severityTexts(source))
}

func severityTexts(records LogRecordSlice) []string {
	got := make([]string, records.Len())
	for index := range records.Len() {
		got[index] = records.At(index).SeverityText()
	}
	return got
}

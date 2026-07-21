// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	sourceLogs := NewLogs()
	source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	first := source.AppendEmpty()
	first.SetSeverityText("first")
	source.AppendEmpty().SetSeverityText("second")
	source.AppendEmpty().SetSeverityText("third")
	source.AppendEmpty().SetSeverityText("fourth")

	destinationLogs := NewLogs()
	destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	destination.AppendEmpty().SetSeverityText("existing")

	source.MoveFirstNTo(2, destination)
	source.MoveFirstNTo(1, destination)

	require.Equal(t, 1, source.Len())
	require.Equal(t, "fourth", source.At(0).SeverityText())
	require.Equal(t, []string{"existing", "first", "second", "third"}, logRecordSeverityTexts(destination))

	destinationLogs.MarkReadOnly()
	first.SetSeverityText("changed source handle")
	require.Equal(t, "first", destination.At(1).SeverityText())
}

func logRecordSeverityTexts(records LogRecordSlice) []string {
	texts := make([]string, records.Len())
	for index := range records.Len() {
		texts[index] = records.At(index).SeverityText()
	}
	return texts
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	source := NewLogRecordSlice()
	source.AppendEmpty().SetSeverityText("first")
	source.AppendEmpty().SetSeverityText("second")
	source.AppendEmpty().SetSeverityText("third")
	source.AppendEmpty().SetSeverityText("fourth")
	destination := NewLogRecordSlice()
	destination.AppendEmpty().SetSeverityText("existing")

	source.MoveFirstNTo(2, destination)
	source.MoveFirstNTo(1, destination)

	assert.Equal(t, 1, source.Len())
	assert.Equal(t, "fourth", source.At(0).SeverityText())
	assert.Equal(t, 4, destination.Len())
	assert.Equal(t, "existing", destination.At(0).SeverityText())
	assert.Equal(t, "first", destination.At(1).SeverityText())
	assert.Equal(t, "second", destination.At(2).SeverityText())
	assert.Equal(t, "third", destination.At(3).SeverityText())
}

func TestLogRecordSliceMoveFirstNToAliased(t *testing.T) {
	logs := NewLogs()
	source := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	source.AppendEmpty().SetSeverityText("first")
	source.AppendEmpty().SetSeverityText("second")
	source.AppendEmpty().SetSeverityText("third")

	destination := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	source.MoveFirstNTo(1, destination)

	assert.Equal(t, 3, source.Len())
	assert.Equal(t, "first", source.At(0).SeverityText())
	assert.Equal(t, "second", source.At(1).SeverityText())
	assert.Equal(t, "third", source.At(2).SeverityText())
}

func TestLogRecordSliceMoveFirstNToRetainedSource(t *testing.T) {
	sourceLogs := NewLogs()
	source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	retained := source.AppendEmpty()
	retained.SetSeverityText("first")
	source.AppendEmpty().SetSeverityText("second")

	destinationLogs := NewLogs()
	destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	source.MoveFirstNTo(1, destination)
	assert.Equal(t, 1, source.Len())
	assert.Equal(t, "second", source.At(0).SeverityText())
	destinationLogs.MarkReadOnly()

	retained.SetSeverityText("changed")
	assert.Equal(t, "changed", retained.SeverityText())
	assert.Equal(t, "first", destination.At(0).SeverityText())
}

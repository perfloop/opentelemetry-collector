// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/internal/metadata"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	for _, pooling := range []bool{false, true} {
		t.Run("Pooling="+strconv.FormatBool(pooling), func(t *testing.T) {
			previousPooling := metadata.PdataUseProtoPoolingFeatureGate.IsEnabled()
			require.NoError(t, featuregate.GlobalRegistry().Set(metadata.PdataUseProtoPoolingFeatureGate.ID(), pooling))
			t.Cleanup(func() {
				require.NoError(t, featuregate.GlobalRegistry().Set(metadata.PdataUseProtoPoolingFeatureGate.ID(), previousPooling))
			})

			sourceLogs := NewLogs()
			source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			first := source.AppendEmpty()
			first.SetSeverityText("first")
			retainedBody := first.Body()
			retainedBodyBytes := retainedBody.SetEmptyBytes()
			retainedBodyBytes.FromRaw([]byte{1, 2})
			retainedArrayValue := first.Attributes().PutEmptySlice("array").AppendEmpty()
			retainedArrayValue.SetEmptyBytes().FromRaw([]byte{3, 4})
			retainedArrayBytes := retainedArrayValue.Bytes()
			kvList := first.Attributes().PutEmptyMap("kv-list")
			kvList.PutEmptyBytes("bytes").FromRaw([]byte{5, 6})
			retainedKVListValue, found := first.Attributes().Get("kv-list")
			require.True(t, found)
			retainedKVValue, found := retainedKVListValue.Map().Get("bytes")
			require.True(t, found)
			retainedKVBytes := retainedKVValue.Bytes()
			source.AppendEmpty().SetSeverityText("second")
			source.AppendEmpty().SetSeverityText("third")

			destinationLogs := NewLogs()
			destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			destination.AppendEmpty().SetSeverityText("existing")

			source.MoveFirstNTo(1, destination)
			source.MoveFirstNTo(1, destination)

			require.Equal(t, 1, source.Len())
			require.Equal(t, "third", source.At(0).SeverityText())
			require.Equal(t, []string{"existing", "first", "second"}, logRecordSeverityTexts(destination))

			destinationLogs.MarkReadOnly()
			retainedBodyBytes.SetAt(0, 9)
			retainedArrayBytes.SetAt(0, 9)
			retainedKVBytes.SetAt(0, 9)
			first.SetSeverityText("changed source record")
			retainedBody.SetStr("changed source body")
			retainedArrayValue.SetStr("changed source array")
			retainedKVValue.SetStr("changed source kv-list")

			destinationRecord := destination.At(1)
			require.Equal(t, "first", destinationRecord.SeverityText())
			require.Equal(t, []byte{1, 2}, destinationRecord.Body().Bytes().AsRaw())
			arrayValue, found := destinationRecord.Attributes().Get("array")
			require.True(t, found)
			require.Equal(t, []byte{3, 4}, arrayValue.Slice().At(0).Bytes().AsRaw())
			kvListValue, found := destinationRecord.Attributes().Get("kv-list")
			require.True(t, found)
			kvListBytes, found := kvListValue.Map().Get("bytes")
			require.True(t, found)
			require.Equal(t, []byte{5, 6}, kvListBytes.Bytes().AsRaw())
		})
	}
}

func TestLogRecordSliceMoveFirstNToSourceMutation(t *testing.T) {
	sourceLogs := NewLogs()
	source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	sourceRecord := source.AppendEmpty()
	retainedBody := sourceRecord.Body()
	retainedBody.SetEmptyBytes().FromRaw([]byte{1, 2})
	retainedBodyBytes := retainedBody.Bytes()

	destinationLogs := NewLogs()
	destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	source.MoveFirstNTo(1, destination)
	destinationRecord := destination.At(0)

	retainedBodyBytes.SetAt(0, 9)
	retainedBody.SetStr("changed source body")
	require.Equal(t, []byte{1, 2}, destinationRecord.Body().Bytes().AsRaw())
	destinationRecord.SetSeverityText("changed destination")
	require.Equal(t, "changed destination", destination.At(0).SeverityText())
}

func TestLogRecordSliceMoveFirstNToSharedState(t *testing.T) {
	logs := NewLogs()
	scopes := logs.ResourceLogs().AppendEmpty().ScopeLogs()
	source := scopes.AppendEmpty().LogRecords()
	destination := scopes.AppendEmpty().LogRecords()
	sourceRecord := source.AppendEmpty()
	sourceRecord.SetSeverityText("source")

	source.MoveFirstNTo(1, destination)
	sourceRecord.SetSeverityText("changed source")

	require.Equal(t, "source", destination.At(0).SeverityText())
	require.Equal(t, "changed source", sourceRecord.SeverityText())
	destinationRecord := destination.At(0)
	destinationRecord.SetSeverityText("changed destination")
	require.Equal(t, "changed destination", destination.At(0).SeverityText())
}

func TestLogRecordSliceMoveFirstNToRetransfer(t *testing.T) {
	sourceLogs := NewLogs()
	source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	sourceRecord := source.AppendEmpty()
	retainedBody := sourceRecord.Body()
	retainedBody.SetEmptyBytes().FromRaw([]byte{1, 2})
	retainedBodyBytes := retainedBody.Bytes()

	destinationLogs := NewLogs()
	destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	nextLogs := NewLogs()
	next := nextLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()

	source.MoveFirstNTo(1, destination)
	destination.MoveAndAppendTo(next)
	nextLogs.MarkReadOnly()
	retainedBodyBytes.SetAt(0, 9)
	retainedBody.SetStr("changed source body")

	require.Equal(t, []byte{1, 2}, next.At(0).Body().Bytes().AsRaw())
}

func TestLogRecordSliceMoveFirstNToParentRetransfer(t *testing.T) {
	moves := []struct {
		name string
		move func(Logs, Logs)
	}{
		{
			name: "Logs",
			move: func(destination, next Logs) {
				destination.MoveTo(next)
			},
		},
		{
			name: "ResourceLogs",
			move: func(destination, next Logs) {
				destination.ResourceLogs().At(0).MoveTo(next.ResourceLogs().AppendEmpty())
			},
		},
		{
			name: "ResourceLogsSlice",
			move: func(destination, next Logs) {
				destination.ResourceLogs().MoveAndAppendTo(next.ResourceLogs())
			},
		},
		{
			name: "ScopeLogs",
			move: func(destination, next Logs) {
				nextScope := next.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
				destination.ResourceLogs().At(0).ScopeLogs().At(0).MoveTo(nextScope)
			},
		},
		{
			name: "ScopeLogsSlice",
			move: func(destination, next Logs) {
				nextScopeLogs := next.ResourceLogs().AppendEmpty().ScopeLogs()
				destination.ResourceLogs().At(0).ScopeLogs().MoveAndAppendTo(nextScopeLogs)
			},
		},
	}

	for _, test := range moves {
		t.Run(test.name, func(t *testing.T) {
			sourceLogs := NewLogs()
			source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			retainedBody := source.AppendEmpty().Body()
			retainedBody.SetEmptyBytes().FromRaw([]byte{1, 2})
			retainedBodyBytes := retainedBody.Bytes()

			destinationLogs := NewLogs()
			destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			nextLogs := NewLogs()

			source.MoveFirstNTo(1, destination)
			test.move(destinationLogs, nextLogs)
			nextLogs.MarkReadOnly()
			retainedBodyBytes.SetAt(0, 9)

			nextRecord := nextLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			require.Equal(t, []byte{1, 2}, nextRecord.Body().Bytes().AsRaw())
		})
	}
}

func TestLogRecordSliceMoveFirstNToRecordRetransfer(t *testing.T) {
	sourceLogs := NewLogs()
	source := sourceLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	sourceRecord := source.AppendEmpty()
	sourceRecord.SetSeverityText("before move")
	retainedBody := sourceRecord.Body()
	retainedBody.SetEmptyBytes().FromRaw([]byte{1, 2})
	retainedBodyBytes := retainedBody.Bytes()

	destinationLogs := NewLogs()
	destination := destinationLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	nextLogs := NewLogs()
	next := nextLogs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	nextRecord := next.AppendEmpty()

	source.MoveFirstNTo(1, destination)
	destination.At(0).MoveTo(nextRecord)
	nextLogs.MarkReadOnly()
	retainedBodyBytes.SetAt(0, 9)
	retainedBody.SetStr("changed source body")
	sourceRecord.SetSeverityText("changed source record")

	require.Equal(t, "changed source record", sourceRecord.SeverityText())
	require.Equal(t, "before move", nextRecord.SeverityText())
	require.Equal(t, []byte{1, 2}, nextRecord.Body().Bytes().AsRaw())
}

func logRecordSeverityTexts(records LogRecordSlice) []string {
	texts := make([]string, records.Len())
	for index := range records.Len() {
		texts[index] = records.At(index).SeverityText()
	}
	return texts
}

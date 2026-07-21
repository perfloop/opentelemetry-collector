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
			first.Attributes().PutStr("retained", "first")
			retainedAttribute, found := first.Attributes().Get("retained")
			require.True(t, found)
			retainedBytes := first.Attributes().PutEmptyBytes("bytes")
			retainedBytes.FromRaw([]byte{1, 2})
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
			retainedAttribute.SetStr("changed source attribute")
			retainedBytes.SetAt(0, 9)
			require.Equal(t, "first", destination.At(1).SeverityText())
			destinationAttribute, found := destination.At(1).Attributes().Get("retained")
			require.True(t, found)
			require.Equal(t, "first", destinationAttribute.Str())
			destinationBytes, found := destination.At(1).Attributes().Get("bytes")
			require.True(t, found)
			require.Equal(t, []byte{1, 2}, destinationBytes.Bytes().AsRaw())
		})
	}
}

func TestLogRecordSliceMoveFirstNToAliased(t *testing.T) {
	logs := NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	records.AppendEmpty().SetSeverityText("first")
	records.AppendEmpty().SetSeverityText("second")

	records.MoveFirstNTo(1, records)
	require.Equal(t, []string{"first", "second"}, logRecordSeverityTexts(records))
}

func logRecordSeverityTexts(records LogRecordSlice) []string {
	texts := make([]string, records.Len())
	for index := range records.Len() {
		texts[index] = records.At(index).SeverityText()
	}
	return texts
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	benchmarkGuardLogRecords = 1024
	benchmarkGuardLogPage    = 32
)

func BenchmarkSplitLogsMaxSize1024Page32(b *testing.B) {
	b.ReportAllocs()

	for iteration := 0; b.Loop(); iteration++ {
		b.StopTimer()
		logs := plog.NewLogs()
		resourceLogs := logs.ResourceLogs().AppendEmpty()
		resourceLogs.Resource().Attributes().PutStr("benchmark.resource", "resource")
		resourceLogs.SetSchemaUrl("https://example.com/resource")
		scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
		scopeLogs.Scope().SetName("benchmark-scope")
		scopeLogs.Scope().Attributes().PutStr("benchmark.scope", "scope")
		scopeLogs.SetSchemaUrl("https://example.com/scope")
		for recordIndex := range benchmarkGuardLogRecords {
			record := scopeLogs.LogRecords().AppendEmpty()
			record.SetSeverityText(benchmarkLogRecordText(iteration, recordIndex))
			record.Body().SetStr("benchmark body")
			record.Attributes().PutInt("benchmark.record", int64(recordIndex))
		}
		pages := make([]plog.Logs, 0, benchmarkGuardLogRecords/benchmarkGuardLogPage)

		b.StartTimer()
		for remaining := benchmarkGuardLogRecords; remaining > 0; remaining -= benchmarkGuardLogPage {
			pages = append(pages, splitLogs(benchmarkGuardLogPage, logs))
		}
		b.StopTimer()

		recordIndex := 0
		for _, page := range pages {
			resourceLogs := page.ResourceLogs()
			require.Equal(b, 1, resourceLogs.Len())
			scopeLogs := resourceLogs.At(0).ScopeLogs()
			require.Equal(b, 1, scopeLogs.Len())
			records := scopeLogs.At(0).LogRecords()
			require.Equal(b, benchmarkGuardLogPage, records.Len())
			for index := range records.Len() {
				require.Equal(b, benchmarkLogRecordText(iteration, recordIndex), records.At(index).SeverityText())
				recordIndex++
			}
		}
		require.Equal(b, benchmarkGuardLogRecords, recordIndex)
		b.StartTimer()
	}
}

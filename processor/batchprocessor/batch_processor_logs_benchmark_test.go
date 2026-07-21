// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	benchmarkCappedLogRecords = 4096
	benchmarkCappedLogPage    = 64
)

func BenchmarkSplitLogsMaxSize4096(b *testing.B) {
	b.ReportAllocs()

	for iteration := 0; b.Loop(); iteration++ {
		b.StopTimer()
		logs := benchmarkCappedLogs(iteration)
		pages := make([]plog.Logs, 0, benchmarkCappedLogRecords/benchmarkCappedLogPage)

		b.StartTimer()
		for remaining := benchmarkCappedLogRecords; remaining > 0; remaining -= benchmarkCappedLogPage {
			pages = append(pages, splitLogs(benchmarkCappedLogPage, logs))
		}
		b.StopTimer()

		assertBenchmarkCappedLogs(b, pages, iteration)
		b.StartTimer()
	}
}

func benchmarkCappedLogs(iteration int) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("benchmark.resource", "resource")
	resourceLogs.SetSchemaUrl("https://example.com/resource")
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName("benchmark-scope")
	scopeLogs.Scope().Attributes().PutStr("benchmark.scope", "scope")
	scopeLogs.SetSchemaUrl("https://example.com/scope")

	for recordIndex := range benchmarkCappedLogRecords {
		record := scopeLogs.LogRecords().AppendEmpty()
		record.SetSeverityText(benchmarkLogRecordText(iteration, recordIndex))
		record.Body().SetStr("benchmark body")
		record.Attributes().PutInt("benchmark.record", int64(recordIndex))
	}

	return logs
}

func assertBenchmarkCappedLogs(b *testing.B, pages []plog.Logs, iteration int) {
	b.Helper()

	recordIndex := 0
	for _, page := range pages {
		resourceLogs := page.ResourceLogs()
		require.Equal(b, 1, resourceLogs.Len())
		scopeLogs := resourceLogs.At(0).ScopeLogs()
		require.Equal(b, 1, scopeLogs.Len())
		records := scopeLogs.At(0).LogRecords()
		require.Equal(b, benchmarkCappedLogPage, records.Len())
		for index := range records.Len() {
			require.Equal(b, benchmarkLogRecordText(iteration, recordIndex), records.At(index).SeverityText())
			recordIndex++
		}
	}
	require.Equal(b, benchmarkCappedLogRecords, recordIndex)
}

func benchmarkLogRecordText(iteration, recordIndex int) string {
	return fmt.Sprintf("benchmark-%d-record-%d", iteration, recordIndex)
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

const (
	benchmarkCappedLogRecords = 4096
	benchmarkCappedLogPage    = 64
)

func BenchmarkBatchLogsMaxSize4096(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()

	for iteration := 0; b.Loop(); iteration++ {
		b.StopTimer()
		sink := new(batchLogsBenchmarkSink)
		logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
			SendBatchMaxSize: benchmarkCappedLogPage,
		}, sink)
		require.NoError(b, err)
		require.NoError(b, logsProcessor.Start(ctx, componenttest.NewNopHost()))
		logs := benchmarkCappedLogs(iteration)

		b.StartTimer()
		require.NoError(b, logsProcessor.ConsumeLogs(ctx, logs))
		require.NoError(b, logsProcessor.Shutdown(ctx))
		b.StopTimer()

		require.Len(b, sink.logs, benchmarkCappedLogRecords/benchmarkCappedLogPage)
		assertBenchmarkCappedLogs(b, sink.logs, iteration)
		b.StartTimer()
	}
}

type batchLogsBenchmarkSink struct {
	logs []plog.Logs
}

func (*batchLogsBenchmarkSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *batchLogsBenchmarkSink) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	s.logs = append(s.logs, logs)
	return nil
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

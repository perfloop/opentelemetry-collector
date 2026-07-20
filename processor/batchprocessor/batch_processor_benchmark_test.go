// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

const benchmarkBatchLogsMaxSize = 64

func BenchmarkBatchLogsMaxSize(b *testing.B) {
	for _, testCase := range []struct {
		name      string
		resources int
		scopes    int
		records   int
	}{
		{name: "one_resource_one_scope_1024", resources: 1, scopes: 1, records: 1024},
		{name: "one_resource_one_scope_4096", resources: 1, scopes: 1, records: 4096},
		{name: "one_resource_one_scope_8192", resources: 1, scopes: 1, records: 8192},
		{name: "many_resources_4096", resources: 64, scopes: 1, records: 64},
		{name: "one_resource_many_scopes_4096", resources: 1, scopes: 64, records: 64},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			benchmarkCappedLogs(b, testCase.resources, testCase.scopes, testCase.records)
		})
	}
}

func benchmarkCappedLogs(b *testing.B, resources, scopes, records int) {
	b.Helper()
	b.ReportAllocs()

	const maxBatchSize = benchmarkBatchLogsMaxSize
	recordCount := resources * scopes * records
	expectedPages := (recordCount + maxBatchSize - 1) / maxBatchSize
	ctx := context.Background()
	iteration := 0

	for b.Loop() {
		b.StopTimer()
		sink := &batchLogsBenchmarkSink{
			done:     make(chan struct{}),
			expected: recordCount,
		}
		logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
			SendBatchMaxSize: maxBatchSize,
		}, sink)
		require.NoError(b, err)
		require.NoError(b, logsProcessor.Start(ctx, componenttest.NewNopHost()))
		logs := newCappedTestLogs(resources, scopes, records)
		firstLogs := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
		firstLogs.At(0).SetSeverityText(strconv.Itoa(iteration))
		iteration++

		b.StartTimer()
		require.NoError(b, logsProcessor.ConsumeLogs(ctx, logs))
		<-sink.done
		b.StopTimer()

		require.NoError(b, logsProcessor.Shutdown(ctx))
		require.Equal(b, recordCount, sink.records)
		require.Equal(b, expectedPages, sink.pages)
		b.StartTimer()
	}
}

type batchLogsBenchmarkSink struct {
	done     chan struct{}
	expected int
	records  int
	pages    int
}

func (*batchLogsBenchmarkSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *batchLogsBenchmarkSink) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	s.records += logs.LogRecordCount()
	s.pages++
	if s.records == s.expected {
		close(s.done)
	}
	return nil
}

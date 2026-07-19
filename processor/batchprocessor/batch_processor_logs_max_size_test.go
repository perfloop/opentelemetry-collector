// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

const benchmarkLogsMaxSize = 64

func TestBatchLogsMaxSizePreservesOrderAndMetadata(t *testing.T) {
	const (
		resourceCount = 3
		scopeCount    = 2
		logsPerScope  = 7
		sendBatchMax  = 5
		metadataKey   = "tenant"
	)

	sink := new(capturingLogsSink)
	cfg := &Config{
		SendBatchMaxSize: sendBatchMax,
		MetadataKeys:     []string{metadataKey},
	}
	require.NoError(t, cfg.Validate())

	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))

	tenants := []string{"tenant-a", "tenant-b"}
	expectedByTenant := make(map[string][]string, len(tenants))
	for _, tenant := range tenants {
		ld, expected := newMaxSizeTestLogs(tenant, resourceCount, scopeCount, logsPerScope)
		expectedByTenant[tenant] = expected
		requestCtx := client.NewContext(ctx, client.Info{
			Metadata: client.NewMetadata(map[string][]string{metadataKey: {tenant}}),
		})
		require.NoError(t, logsProcessor.ConsumeLogs(requestCtx, ld))
	}

	require.NoError(t, logsProcessor.Shutdown(ctx))

	actualByTenant := make(map[string][]string, len(tenants))
	pagesByTenant := make(map[string]int, len(tenants))
	for _, received := range sink.all() {
		require.Contains(t, expectedByTenant, received.tenant)
		require.Positive(t, received.logs.LogRecordCount())
		require.LessOrEqual(t, received.logs.LogRecordCount(), sendBatchMax)

		pagesByTenant[received.tenant]++
		actualByTenant[received.tenant] = append(actualByTenant[received.tenant], logIDs(t, received.tenant, received.logs)...)
	}

	for _, tenant := range tenants {
		expected := expectedByTenant[tenant]
		require.Equal(t, expected, actualByTenant[tenant])
		require.Equal(t, (len(expected)+sendBatchMax-1)/sendBatchMax, pagesByTenant[tenant])
	}
}

func TestBatchLogsMaxSizeRetainsLogsUntilAllPagesExported(t *testing.T) {
	previousPooling := pref.UseProtoPooling.IsEnabled()
	require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), previousPooling))
	})

	const sendBatchMax = 5
	ctx := context.Background()
	sink := new(capturingLogsSink)
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: sendBatchMax,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))

	ld, expected := newMaxSizeTestLogs("ownership", 2, 2, 7)
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, ld))
	pref.UnrefLogs(ld)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	var actual []string
	for _, received := range sink.all() {
		require.Positive(t, received.logs.LogRecordCount())
		require.LessOrEqual(t, received.logs.LogRecordCount(), sendBatchMax)
		actual = append(actual, logIDs(t, "ownership", received.logs)...)
	}
	require.Equal(t, expected, actual)
	// The caller released its reference immediately after enqueueing. The batcher must
	// release its retained reference after every page has been exported.
	require.Panics(t, func() { pref.UnrefLogs(ld) })
}

func BenchmarkBatchLogsMaxSizeSingleScope1024(b *testing.B) {
	benchmarkBatchLogsMaxSize(b, 1024, newSingleScopeBenchmarkLogs(1024))
}

func BenchmarkBatchLogsMaxSizeSingleScope4096(b *testing.B) {
	benchmarkBatchLogsMaxSize(b, 4096, newSingleScopeBenchmarkLogs(4096))
}

func BenchmarkBatchLogsMaxSizeSingleScope8192(b *testing.B) {
	benchmarkBatchLogsMaxSize(b, 8192, newSingleScopeBenchmarkLogs(8192))
}

func BenchmarkBatchLogsMaxSizeFragmented4096(b *testing.B) {
	benchmarkBatchLogsMaxSize(b, 4096, newFragmentedBenchmarkLogs)
}

func benchmarkBatchLogsMaxSize(b *testing.B, logCount int, newLogs func(int) plog.Logs) {
	b.Helper()
	require.Zero(b, logCount%benchmarkLogsMaxSize)

	ctx := context.Background()
	sink := &benchmarkLogsSink{pages: make(chan struct{}, logCount/benchmarkLogsMaxSize)}
	cfg := &Config{SendBatchMaxSize: benchmarkLogsMaxSize}
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(b, err)
	require.NoError(b, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, logsProcessor.Shutdown(ctx)) })

	iteration := 0
	for b.Loop() {
		b.StopTimer()
		ld := newLogs(iteration)
		iteration++
		require.Equal(b, logCount, ld.LogRecordCount())
		b.StartTimer()

		require.NoError(b, logsProcessor.ConsumeLogs(ctx, ld))
		for range logCount / benchmarkLogsMaxSize {
			<-sink.pages
		}
	}
}

type benchmarkLogsSink struct {
	pages chan struct{}
}

func (*benchmarkLogsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *benchmarkLogsSink) ConsumeLogs(context.Context, plog.Logs) error {
	s.pages <- struct{}{}
	return nil
}

type capturedLogs struct {
	tenant string
	logs   plog.Logs
}

type capturingLogsSink struct {
	mu       sync.Mutex
	received []capturedLogs
}

func (*capturingLogsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *capturingLogsSink) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	copied := plog.NewLogs()
	ld.CopyTo(copied)

	metadata := client.FromContext(ctx).Metadata.Get("tenant")
	tenant := ""
	if len(metadata) == 1 {
		tenant = metadata[0]
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = append(s.received, capturedLogs{tenant: tenant, logs: copied})
	return nil
}

func (s *capturingLogsSink) all() []capturedLogs {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedLogs(nil), s.received...)
}

func newMaxSizeTestLogs(tenant string, resourceCount, scopeCount, logsPerScope int) (plog.Logs, []string) {
	ld := plog.NewLogs()
	ids := make([]string, 0, resourceCount*scopeCount*logsPerScope)
	for resourceIndex := range resourceCount {
		resourceName := fmt.Sprintf("resource-%d", resourceIndex)
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("resource", resourceName)
		rl.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))

		for scopeIndex := range scopeCount {
			scopeName := fmt.Sprintf("scope-%d", scopeIndex)
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(scopeName)
			sl.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))

			for logIndex := range logsPerScope {
				id := fmt.Sprintf("%s/%s/%s/log-%d", tenant, resourceName, scopeName, logIndex)
				sl.LogRecords().AppendEmpty().SetSeverityText(id)
				ids = append(ids, id)
			}
		}
	}
	return ld, ids
}

func logIDs(t *testing.T, tenant string, ld plog.Logs) []string {
	t.Helper()
	var ids []string
	resourceLogs := ld.ResourceLogs()
	require.Positive(t, resourceLogs.Len())
	for resourceIndex := range resourceLogs.Len() {
		rl := resourceLogs.At(resourceIndex)
		resourceName, ok := rl.Resource().Attributes().Get("resource")
		require.True(t, ok)
		require.NotEmpty(t, resourceName.Str())
		require.NotEmpty(t, rl.SchemaUrl())

		scopeLogs := rl.ScopeLogs()
		require.Positive(t, scopeLogs.Len())
		for scopeIndex := range scopeLogs.Len() {
			sl := scopeLogs.At(scopeIndex)
			require.NotEmpty(t, sl.Scope().Name())
			require.NotEmpty(t, sl.SchemaUrl())

			prefix := fmt.Sprintf("%s/%s/%s/", tenant, resourceName.Str(), sl.Scope().Name())
			logRecords := sl.LogRecords()
			require.Positive(t, logRecords.Len())
			for logIndex := range logRecords.Len() {
				id := logRecords.At(logIndex).SeverityText()
				require.True(t, strings.HasPrefix(id, prefix), "log record %q does not belong to its resource and scope", id)
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func newSingleScopeBenchmarkLogs(logCount int) func(int) plog.Logs {
	return func(iteration int) plog.Logs {
		ld := testdata.GenerateLogs(logCount)
		ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetSeverityText(fmt.Sprintf("benchmark-%d", iteration))
		return ld
	}
}

func newFragmentedBenchmarkLogs(iteration int) plog.Logs {
	const (
		resourceCount = 16
		scopeCount    = 8
		logsPerScope  = 32
	)

	ld := plog.NewLogs()
	for resourceIndex := range resourceCount {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("resource", fmt.Sprintf("resource-%d", resourceIndex))
		rl.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))
		for scopeIndex := range scopeCount {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(fmt.Sprintf("scope-%d", scopeIndex))
			sl.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))
			for logIndex := range logsPerScope {
				logRecord := sl.LogRecords().AppendEmpty()
				if resourceIndex == 0 && scopeIndex == 0 && logIndex == 0 {
					logRecord.SetSeverityText(fmt.Sprintf("benchmark-%d", iteration))
				}
			}
		}
	}
	return ld
}

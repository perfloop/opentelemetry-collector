// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestBatchLogsMaxSizePagesMatchFIFO(t *testing.T) {
	testCases := []struct {
		name         string
		resources    int
		scopes       int
		records      int
		maxBatchSize int
	}{
		{name: "one_resource_one_scope", resources: 1, scopes: 1, records: 17, maxBatchSize: 5},
		{name: "many_resources", resources: 3, scopes: 1, records: 4, maxBatchSize: 5},
		{name: "one_resource_many_scopes", resources: 1, scopes: 3, records: 4, maxBatchSize: 5},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			expected := expectedCappedLogPages(testCase.resources, testCase.scopes, testCase.records, testCase.maxBatchSize)
			actual := consumeCappedLogs(t, newCappedTestLogs(testCase.resources, testCase.scopes, testCase.records), &Config{
				SendBatchMaxSize: uint32(testCase.maxBatchSize),
			})

			assertCappedLogPages(t, expected, actual, testCase.maxBatchSize)
		})
	}
}

func TestBatchLogsMaxSizeResidualVisits(t *testing.T) {
	const maxBatchSize = 64
	actual := consumeCappedLogs(t, newCappedTestLogs(1, 1, 4096), &Config{
		SendBatchMaxSize: maxBatchSize,
	})

	require.Len(t, actual, 4096/maxBatchSize)
	for _, page := range actual {
		require.Equal(t, maxBatchSize, page.LogRecordCount())
	}
}

func TestBatchLogsMaxSizePooledOwnership(t *testing.T) {
	previousPooling := pref.UseProtoPooling.IsEnabled()
	require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(pref.UseProtoPooling.ID(), previousPooling))
	})

	const maxBatchSize = 5
	input := newCappedTestLogs(1, 1, 27)
	expected := expectedCappedLogPages(1, 1, 27, maxBatchSize)

	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: maxBatchSize,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	pref.UnrefLogs(input)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	assertCappedLogPages(t, expected, sink.AllLogs(), maxBatchSize)
	// The batcher must release the reference it retained while the capped pages were exported.
	require.Panics(t, func() { pref.UnrefLogs(input) })
}

func TestBatchLogsMaxSizeMetadataShards(t *testing.T) {
	const maxBatchSize = 5
	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		SendBatchMaxSize: maxBatchSize,
		MetadataKeys:     []string{"tenant"},
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))

	expected := map[string][]plog.Logs{}
	for _, tenant := range []string{"a", "b"} {
		expected[tenant] = expectedCappedLogPages(1, 1, 12, maxBatchSize)
		tenantCtx := client.NewContext(ctx, client.Info{
			Metadata: client.NewMetadata(map[string][]string{"tenant": {tenant}}),
		})
		require.NoError(t, logsProcessor.ConsumeLogs(tenantCtx, newCappedTestLogs(1, 1, 12)))
	}
	require.NoError(t, logsProcessor.Shutdown(ctx))

	actual := map[string][]plog.Logs{}
	for index, logs := range sink.AllLogs() {
		tenants := client.FromContext(sink.Contexts()[index]).Metadata.Get("tenant")
		require.Len(t, tenants, 1)
		actual[tenants[0]] = append(actual[tenants[0]], logs)
	}
	for tenant, want := range expected {
		assertCappedLogPages(t, want, actual[tenant], maxBatchSize)
	}
}

func TestBatchLogsMaxSizeTriggers(t *testing.T) {
	t.Run("batch_size", func(t *testing.T) {
		const maxBatchSize = 5
		expected := expectedCappedLogPages(1, 1, 7, maxBatchSize)
		actual := consumeCappedLogs(t, newCappedTestLogs(1, 1, 7), &Config{
			Timeout:          time.Hour,
			SendBatchSize:    maxBatchSize,
			SendBatchMaxSize: maxBatchSize,
		})
		assertCappedLogPages(t, expected, actual, maxBatchSize)
	})

	t.Run("timeout", func(t *testing.T) {
		const maxBatchSize = 5
		sink := new(consumertest.LogsSink)
		ctx := context.Background()
		logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
			Timeout:          5 * time.Millisecond,
			SendBatchSize:    maxBatchSize,
			SendBatchMaxSize: maxBatchSize,
		}, sink)
		require.NoError(t, err)
		require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
		require.NoError(t, logsProcessor.ConsumeLogs(ctx, newCappedTestLogs(1, 1, 4)))
		require.Eventually(t, func() bool { return sink.LogRecordCount() == 4 }, time.Second, time.Millisecond)
		require.NoError(t, logsProcessor.Shutdown(ctx))

		assertCappedLogPages(t, expectedCappedLogPages(1, 1, 4, maxBatchSize), sink.AllLogs(), maxBatchSize)
	})
}

func consumeCappedLogs(t *testing.T, input plog.Logs, cfg *Config) []plog.Logs {
	t.Helper()

	sink := new(consumertest.LogsSink)
	ctx := context.Background()
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	require.NoError(t, logsProcessor.Shutdown(ctx))

	return sink.AllLogs()
}

func expectedCappedLogPages(resources, scopes, records, maxBatchSize int) []plog.Logs {
	pages := []plog.Logs{}
	pageRecords := maxBatchSize
	var page cappedLogPage
	for resourceIndex := range resources {
		for scopeIndex := range scopes {
			for recordIndex := range records {
				if pageRecords == maxBatchSize {
					page = cappedLogPage{logs: plog.NewLogs(), resourceIndex: -1, scopeIndex: -1}
					pages = append(pages, page.logs)
					pageRecords = 0
				}
				page.append(resourceIndex, scopeIndex, recordIndex)
				pageRecords++
			}
		}
	}
	return pages
}

type cappedLogPage struct {
	logs          plog.Logs
	resourceLogs  plog.ResourceLogs
	scopeLogs     plog.ScopeLogs
	resourceIndex int
	scopeIndex    int
}

func (page *cappedLogPage) append(resourceIndex, scopeIndex, recordIndex int) {
	if page.resourceIndex != resourceIndex {
		page.resourceLogs = page.logs.ResourceLogs().AppendEmpty()
		page.resourceLogs.Resource().Attributes().PutStr("resource.name", fmt.Sprintf("resource-%d", resourceIndex))
		page.resourceLogs.Resource().Attributes().PutInt("resource.index", int64(resourceIndex))
		page.resourceLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))
		page.resourceIndex = resourceIndex
		page.scopeIndex = -1
	}
	if page.scopeIndex != scopeIndex {
		page.scopeLogs = page.resourceLogs.ScopeLogs().AppendEmpty()
		page.scopeLogs.Scope().SetName(fmt.Sprintf("scope-%d", scopeIndex))
		page.scopeLogs.Scope().SetVersion(fmt.Sprintf("v%d", scopeIndex))
		page.scopeLogs.Scope().Attributes().PutStr("scope.name", fmt.Sprintf("scope-%d", scopeIndex))
		page.scopeLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))
		page.scopeIndex = scopeIndex
	}
	logRecord := page.scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetSeverityText(fmt.Sprintf("resource-%d/scope-%d/record-%d", resourceIndex, scopeIndex, recordIndex))
	logRecord.Body().SetStr(fmt.Sprintf("body-%d", recordIndex))
	logRecord.Attributes().PutInt("record.index", int64(recordIndex))
}

func assertCappedLogPages(t *testing.T, expected, actual []plog.Logs, maxBatchSize int) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for page := range expected {
		require.LessOrEqual(t, actual[page].LogRecordCount(), maxBatchSize)
		require.Truef(t, pref.EqualLogs(expected[page], actual[page]), "page %d differs", page)
	}
}

func newCappedTestLogs(resources, scopes, records int) plog.Logs {
	logs := plog.NewLogs()
	for resourceIndex := range resources {
		resourceLogs := logs.ResourceLogs().AppendEmpty()
		resourceLogs.Resource().Attributes().PutStr("resource.name", fmt.Sprintf("resource-%d", resourceIndex))
		resourceLogs.Resource().Attributes().PutInt("resource.index", int64(resourceIndex))
		resourceLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/resource/%d", resourceIndex))

		for scopeIndex := range scopes {
			scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
			scopeLogs.Scope().SetName(fmt.Sprintf("scope-%d", scopeIndex))
			scopeLogs.Scope().SetVersion(fmt.Sprintf("v%d", scopeIndex))
			scopeLogs.Scope().Attributes().PutStr("scope.name", fmt.Sprintf("scope-%d", scopeIndex))
			scopeLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/scope/%d", scopeIndex))

			for recordIndex := range records {
				logRecord := scopeLogs.LogRecords().AppendEmpty()
				logRecord.SetSeverityText(fmt.Sprintf("resource-%d/scope-%d/record-%d", resourceIndex, scopeIndex, recordIndex))
				logRecord.Body().SetStr(fmt.Sprintf("body-%d", recordIndex))
				logRecord.Attributes().PutInt("record.index", int64(recordIndex))
			}
		}
	}
	return logs
}

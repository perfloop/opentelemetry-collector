// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestBatchLogsProcessorCappedPageTimeoutRemainder(t *testing.T) {
	const maxBatchSize = 5
	input := testdata.GenerateLogs(7)
	resourceLogs := input.ResourceLogs().At(0)
	resourceLogs.Resource().Attributes().PutStr("resource.attr", "resource-value")
	resourceLogs.SetSchemaUrl("https://example.com/resource")
	scopeLogs := resourceLogs.ScopeLogs().At(0)
	scopeLogs.Scope().SetName("scope-name")
	scopeLogs.Scope().Attributes().PutStr("scope.attr", "scope-value")
	scopeLogs.SetSchemaUrl("https://example.com/scope")
	for index := range scopeLogs.LogRecords().Len() {
		scopeLogs.LogRecords().At(index).SetSeverityText(fmt.Sprintf("record-%d", index))
	}

	ctx := context.Background()
	sink := new(consumertest.LogsSink)
	logsProcessor, err := NewFactory().CreateLogs(ctx, processortest.NewNopSettings(metadata.Type), &Config{
		Timeout:          5 * time.Millisecond,
		SendBatchSize:    maxBatchSize,
		SendBatchMaxSize: maxBatchSize,
	}, sink)
	require.NoError(t, err)
	require.NoError(t, logsProcessor.Start(ctx, componenttest.NewNopHost()))
	require.NoError(t, logsProcessor.ConsumeLogs(ctx, input))
	// The capped first page exercises the specialization; timeout flushes its remainder.
	require.Eventually(t, func() bool { return sink.LogRecordCount() == 7 }, time.Second, time.Millisecond)
	require.NoError(t, logsProcessor.Shutdown(ctx))

	pages := sink.AllLogs()
	require.Len(t, pages, 2)
	for pageIndex, recordCount := range []int{maxBatchSize, 2} {
		pageResourceLogs := pages[pageIndex].ResourceLogs()
		require.Equal(t, 1, pageResourceLogs.Len())
		pageResource := pageResourceLogs.At(0)
		resourceAttribute, ok := pageResource.Resource().Attributes().Get("resource.attr")
		require.True(t, ok)
		require.Equal(t, "resource-value", resourceAttribute.Str())
		require.Equal(t, "https://example.com/resource", pageResource.SchemaUrl())
		pageScopeLogs := pageResource.ScopeLogs()
		require.Equal(t, 1, pageScopeLogs.Len())
		pageScope := pageScopeLogs.At(0)
		require.Equal(t, "scope-name", pageScope.Scope().Name())
		scopeAttribute, ok := pageScope.Scope().Attributes().Get("scope.attr")
		require.True(t, ok)
		require.Equal(t, "scope-value", scopeAttribute.Str())
		require.Equal(t, "https://example.com/scope", pageScope.SchemaUrl())
		pageRecords := pageScope.LogRecords()
		require.Equal(t, recordCount, pageRecords.Len())
		for recordIndex := range recordCount {
			require.Equal(t, fmt.Sprintf("record-%d", pageIndex*maxBatchSize+recordIndex), pageRecords.At(recordIndex).SeverityText())
		}
	}
}

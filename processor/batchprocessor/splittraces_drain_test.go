// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestBatchTracesSplitDrainsCappedTraceData(t *testing.T) {
	const maxSize = 3

	traceData, want := newSplitTraceHierarchy()
	batch := newBatchTraces(nil)
	batch.traceData = traceData
	batch.spanCount = traceData.SpanCount()

	var pages []ptrace.Traces
	for batch.itemCount() > 0 {
		sent, page := batch.split(maxSize)
		require.Positive(t, sent)
		require.LessOrEqual(t, sent, maxSize)
		require.Equal(t, sent, page.SpanCount())
		pages = append(pages, page)
	}

	require.Zero(t, batch.itemCount())
	require.Zero(t, batch.traceData.SpanCount())

	var got []string
	for _, page := range pages {
		got = append(got, splitTraceLabels(page)...)
	}
	require.Equal(t, want, got)
}

func BenchmarkBatchTracesSplitDrainLargeScope(b *testing.B) {
	const (
		spanCount = 32_768
		pageSize  = 128
	)

	b.ReportAllocs()
	batch := newSplitTraceBenchmarkBatch(spanCount)
	pageCount := spanCount / pageSize
	pages := make([]ptrace.Traces, pageCount)
	sent := make([]int, pageCount)

	for b.Loop() {
		for page := range pageCount {
			sent[page], pages[page] = batch.split(pageSize)
		}

		b.StopTimer()
		total := 0
		for page := range pages {
			require.Equal(b, pageSize, sent[page])
			require.Equal(b, sent[page], pages[page].SpanCount())
			total += sent[page]
		}
		require.Equal(b, spanCount, total)
		require.Zero(b, batch.itemCount())

		batch.traceData = joinSplitTraceBenchmarkPages(pages)
		batch.spanCount = spanCount
		b.StartTimer()
	}
}

func newSplitTraceHierarchy() (ptrace.Traces, []string) {
	spanCounts := [][]int{{5, 4}, {2, 3}}
	traceData := ptrace.NewTraces()
	var labels []string

	for resourceIndex, scopes := range spanCounts {
		resourceName := fmt.Sprintf("resource-%d", resourceIndex)
		resourceSchemaURL := fmt.Sprintf("https://example.com/resource-schema/%d", resourceIndex)
		resourceSpans := traceData.ResourceSpans().AppendEmpty()
		resourceSpans.Resource().Attributes().PutStr("service.name", resourceName)
		resourceSpans.SetSchemaUrl(resourceSchemaURL)

		for scopeIndex, spanCount := range scopes {
			scopeName := fmt.Sprintf("scope-%d-%d", resourceIndex, scopeIndex)
			scopeSchemaURL := fmt.Sprintf("https://example.com/scope-schema/%d/%d", resourceIndex, scopeIndex)
			scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
			scopeSpans.Scope().SetName(scopeName)
			scopeSpans.SetSchemaUrl(scopeSchemaURL)

			spans := scopeSpans.Spans()
			for spanIndex := range spanCount {
				spanName := fmt.Sprintf("span-%d-%d-%d", resourceIndex, scopeIndex, spanIndex)
				spans.AppendEmpty().SetName(spanName)
				labels = append(labels, fmt.Sprintf("%s|%s|%s|%s|%s", resourceName, resourceSchemaURL, scopeName, scopeSchemaURL, spanName))
			}
		}
	}

	return traceData, labels
}

func splitTraceLabels(traceData ptrace.Traces) []string {
	var labels []string
	resourceSpans := traceData.ResourceSpans()
	for resourceIndex := 0; resourceIndex < resourceSpans.Len(); resourceIndex++ {
		resourceSpans := resourceSpans.At(resourceIndex)
		resourceName, ok := resourceSpans.Resource().Attributes().Get("service.name")
		if !ok {
			panic("missing resource service.name")
		}

		scopeSpans := resourceSpans.ScopeSpans()
		for scopeIndex := 0; scopeIndex < scopeSpans.Len(); scopeIndex++ {
			scopeSpans := scopeSpans.At(scopeIndex)
			spans := scopeSpans.Spans()
			for spanIndex := 0; spanIndex < spans.Len(); spanIndex++ {
				labels = append(labels, fmt.Sprintf("%s|%s|%s|%s|%s", resourceName.Str(), resourceSpans.SchemaUrl(), scopeSpans.Scope().Name(), scopeSpans.SchemaUrl(), spans.At(spanIndex).Name()))
			}
		}
	}
	return labels
}

func newSplitTraceBenchmarkBatch(spanCount int) *batchTraces {
	batch := newBatchTraces(nil)
	resourceSpans := batch.traceData.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "batch-split-benchmark")
	resourceSpans.SetSchemaUrl("https://opentelemetry.io/schemas/1.27.0")
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scopeSpans.Scope().SetName("batch-split-benchmark")
	scopeSpans.SetSchemaUrl("https://opentelemetry.io/schemas/1.27.0")

	spans := scopeSpans.Spans()
	spans.EnsureCapacity(spanCount)
	for spanIndex := range spanCount {
		spans.AppendEmpty().SetName(fmt.Sprintf("span-%d", spanIndex))
	}
	batch.spanCount = spanCount
	return batch
}

func joinSplitTraceBenchmarkPages(pages []ptrace.Traces) ptrace.Traces {
	traceData := ptrace.NewTraces()
	resourceSpans := traceData.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scopeSpans.Spans().EnsureCapacity(len(pages) * 128)

	for pageIndex := range pages {
		pageResourceSpans := pages[pageIndex].ResourceSpans()
		pageScopeSpans := pageResourceSpans.At(0).ScopeSpans().At(0)
		if pageIndex == 0 {
			pageResourceSpans.At(0).Resource().CopyTo(resourceSpans.Resource())
			resourceSpans.SetSchemaUrl(pageResourceSpans.At(0).SchemaUrl())
			pageScopeSpans.Scope().CopyTo(scopeSpans.Scope())
			scopeSpans.SetSchemaUrl(pageScopeSpans.SchemaUrl())
		}
		pageScopeSpans.Spans().MoveAndAppendTo(scopeSpans.Spans())
	}

	return traceData
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTraceCursorSplit(t *testing.T) {
	const pageSize = 2

	src := ptrace.NewTraces()
	resource0 := src.ResourceSpans().AppendEmpty()
	resource0.Resource().Attributes().PutStr("service.name", "resource-0")
	resource0.SetSchemaUrl("https://example.com/resource/0")
	scope0 := resource0.ScopeSpans().AppendEmpty()
	scope0.Scope().SetName("scope-0")
	scope0.SetSchemaUrl("https://example.com/scope/0")
	for _, name := range []string{"a", "b", "c"} {
		scope0.Spans().AppendEmpty().SetName(name)
	}
	scope1 := resource0.ScopeSpans().AppendEmpty()
	scope1.Scope().SetName("scope-1")
	scope1.SetSchemaUrl("https://example.com/scope/1")
	scope1.Spans().AppendEmpty().SetName("d")

	resource1 := src.ResourceSpans().AppendEmpty()
	resource1.Resource().Attributes().PutStr("service.name", "resource-1")
	resource1.SetSchemaUrl("https://example.com/resource/1")
	scope2 := resource1.ScopeSpans().AppendEmpty()
	scope2.Scope().SetName("scope-2")
	scope2.SetSchemaUrl("https://example.com/scope/2")
	for _, name := range []string{"e", "f"} {
		scope2.Spans().AppendEmpty().SetName(name)
	}

	cursor := traceCursor{}
	remaining := src.SpanCount()
	var got []string
	for remaining > 0 {
		size := min(pageSize, remaining)
		page := cursor.split(size, src)
		require.Equal(t, size, page.SpanCount())
		got = append(got, traceCursorLabels(page)...)
		remaining -= size
	}

	require.Equal(t, []string{
		"resource-0|https://example.com/resource/0|scope-0|https://example.com/scope/0|a",
		"resource-0|https://example.com/resource/0|scope-0|https://example.com/scope/0|b",
		"resource-0|https://example.com/resource/0|scope-0|https://example.com/scope/0|c",
		"resource-0|https://example.com/resource/0|scope-1|https://example.com/scope/1|d",
		"resource-1|https://example.com/resource/1|scope-2|https://example.com/scope/2|e",
		"resource-1|https://example.com/resource/1|scope-2|https://example.com/scope/2|f",
	}, got)
}

func traceCursorLabels(traceData ptrace.Traces) []string {
	var labels []string
	resources := traceData.ResourceSpans()
	for resourceIndex := 0; resourceIndex < resources.Len(); resourceIndex++ {
		resource := resources.At(resourceIndex)
		resourceName, ok := resource.Resource().Attributes().Get("service.name")
		if !ok {
			panic("resource has no service.name")
		}

		scopes := resource.ScopeSpans()
		for scopeIndex := 0; scopeIndex < scopes.Len(); scopeIndex++ {
			scope := scopes.At(scopeIndex)
			spans := scope.Spans()
			for spanIndex := 0; spanIndex < spans.Len(); spanIndex++ {
				labels = append(labels, resourceName.Str()+"|"+resource.SchemaUrl()+"|"+scope.Scope().Name()+"|"+scope.SchemaUrl()+"|"+spans.At(spanIndex).Name())
			}
		}
	}
	return labels
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import "go.opentelemetry.io/collector/pdata/ptrace"

// traceCursor records the next span to split from a trace batch. It leaves
// moved elements in their source slices so later pages can resume at the
// cursor without compacting the retained tail.
type traceCursor struct {
	resourceIndex int
	scopeIndex    int
	spanIndex     int
	active        bool
}

// split moves up to size spans from src into a new trace page and advances the
// cursor. Whole resource and scope groups move directly when they fit in the
// page; only a group that crosses a page boundary is moved span by span.
func (tc *traceCursor) split(size int, src ptrace.Traces) ptrace.Traces {
	dest := ptrace.NewTraces()
	resources := src.ResourceSpans()
	moved := 0

	var destResource ptrace.ResourceSpans
	hasDestResource := false

	for moved < size {
		if tc.resourceIndex >= resources.Len() {
			panic("trace cursor exhausted before filling page")
		}

		srcResource := resources.At(tc.resourceIndex)
		scopes := srcResource.ScopeSpans()
		if tc.scopeIndex >= scopes.Len() {
			tc.resourceIndex++
			tc.scopeIndex = 0
			tc.spanIndex = 0
			hasDestResource = false
			continue
		}

		remaining := size - moved
		if tc.scopeIndex == 0 && tc.spanIndex == 0 {
			resourceCount := resourceSC(srcResource)
			if resourceCount <= remaining {
				srcResource.MoveTo(dest.ResourceSpans().AppendEmpty())
				moved += resourceCount
				tc.resourceIndex++
				tc.scopeIndex = 0
				tc.spanIndex = 0
				hasDestResource = false
				continue
			}
		}

		if !hasDestResource {
			destResource = dest.ResourceSpans().AppendEmpty()
			srcResource.Resource().CopyTo(destResource.Resource())
			destResource.SetSchemaUrl(srcResource.SchemaUrl())
			hasDestResource = true
		}

		srcScope := scopes.At(tc.scopeIndex)
		spans := srcScope.Spans()
		scopeCount := spans.Len()
		if tc.spanIndex == 0 && scopeCount <= remaining {
			srcScope.MoveTo(destResource.ScopeSpans().AppendEmpty())
			moved += scopeCount
			tc.scopeIndex++
			continue
		}

		destScope := destResource.ScopeSpans().AppendEmpty()
		srcScope.Scope().CopyTo(destScope.Scope())
		destScope.SetSchemaUrl(srcScope.SchemaUrl())
		spansToMove := min(spans.Len()-tc.spanIndex, remaining)
		destSpans := destScope.Spans()
		destSpans.EnsureCapacity(spansToMove)
		for tc.spanIndex < spans.Len() && moved < size {
			spans.At(tc.spanIndex).MoveTo(destSpans.AppendEmpty())
			tc.spanIndex++
			moved++
		}
		if tc.spanIndex == spans.Len() {
			tc.scopeIndex++
			tc.spanIndex = 0
		}
	}

	return dest
}

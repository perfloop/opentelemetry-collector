// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
)

func TestSplitLogs_noop(t *testing.T) {
	td := testdata.GenerateLogs(20)
	splitSize := 40
	split := splitLogs(splitSize, td)
	assert.Equal(t, td, split)

	i := 0
	td.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().RemoveIf(func(plog.LogRecord) bool {
		i++
		return i > 5
	})
	assert.Equal(t, td, split)
}

func TestSplitLogs(t *testing.T) {
	ld := testdata.GenerateLogs(20)
	logs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(0, i))
	}
	cp := plog.NewLogs()
	cpLogs := cp.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	cpLogs.EnsureCapacity(5)
	ld.ResourceLogs().At(0).Resource().CopyTo(
		cp.ResourceLogs().At(0).Resource())
	ld.ResourceLogs().At(0).ScopeLogs().At(0).Scope().CopyTo(
		cp.ResourceLogs().At(0).ScopeLogs().At(0).Scope())
	logs.At(0).CopyTo(cpLogs.AppendEmpty())
	logs.At(1).CopyTo(cpLogs.AppendEmpty())
	logs.At(2).CopyTo(cpLogs.AppendEmpty())
	logs.At(3).CopyTo(cpLogs.AppendEmpty())
	logs.At(4).CopyTo(cpLogs.AppendEmpty())

	splitSize := 5
	split := splitLogs(splitSize, ld)
	assert.Equal(t, splitSize, split.LogRecordCount())
	assert.True(t, pref.EqualLogs(cp, split))
	assert.Equal(t, 15, ld.LogRecordCount())
	assert.Equal(t, "test-log-int-0-0", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-4", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())

	split = splitLogs(splitSize, ld)
	assert.Equal(t, 10, ld.LogRecordCount())
	assert.Equal(t, "test-log-int-0-5", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-9", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())

	split = splitLogs(splitSize, ld)
	assert.Equal(t, 5, ld.LogRecordCount())
	assert.Equal(t, "test-log-int-0-10", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-14", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())

	split = splitLogs(splitSize, ld)
	assert.Equal(t, 5, ld.LogRecordCount())
	assert.Equal(t, "test-log-int-0-15", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-19", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())
}

func TestSplitLogsMultipleResourceLogs(t *testing.T) {
	td := testdata.GenerateLogs(20)
	logs := td.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(0, i))
	}
	// add second index to resource logs
	testdata.GenerateLogs(20).
		ResourceLogs().At(0).CopyTo(td.ResourceLogs().AppendEmpty())
	logs = td.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(1, i))
	}

	splitSize := 5
	split := splitLogs(splitSize, td)
	assert.Equal(t, splitSize, split.LogRecordCount())
	assert.Equal(t, 35, td.LogRecordCount())
	assert.Equal(t, "test-log-int-0-0", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-4", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())
}

func TestSplitLogsMultipleResourceLogs_split_size_greater_than_log_size(t *testing.T) {
	td := testdata.GenerateLogs(20)
	logs := td.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(0, i))
	}
	// add second index to resource logs
	testdata.GenerateLogs(20).
		ResourceLogs().At(0).CopyTo(td.ResourceLogs().AppendEmpty())
	logs = td.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(1, i))
	}

	splitSize := 25
	split := splitLogs(splitSize, td)
	assert.Equal(t, splitSize, split.LogRecordCount())
	assert.Equal(t, 40-splitSize, td.LogRecordCount())
	assert.Equal(t, 1, td.ResourceLogs().Len())
	assert.Equal(t, "test-log-int-0-0", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-19", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(19).SeverityText())
	assert.Equal(t, "test-log-int-1-0", split.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-1-4", split.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords().At(4).SeverityText())
}

func TestSplitLogsMultipleILL(t *testing.T) {
	td := testdata.GenerateLogs(20)
	logs := td.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(0, i))
	}
	// add second index to ILL
	td.ResourceLogs().At(0).ScopeLogs().At(0).
		CopyTo(td.ResourceLogs().At(0).ScopeLogs().AppendEmpty())
	logs = td.ResourceLogs().At(0).ScopeLogs().At(1).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(1, i))
	}

	// add third index to ILL
	td.ResourceLogs().At(0).ScopeLogs().At(0).
		CopyTo(td.ResourceLogs().At(0).ScopeLogs().AppendEmpty())
	logs = td.ResourceLogs().At(0).ScopeLogs().At(2).LogRecords()
	for i := 0; i < logs.Len(); i++ {
		logs.At(i).SetSeverityText(getTestLogSeverityText(2, i))
	}

	splitSize := 40
	split := splitLogs(splitSize, td)
	assert.Equal(t, splitSize, split.LogRecordCount())
	assert.Equal(t, 20, td.LogRecordCount())
	assert.Equal(t, "test-log-int-0-0", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SeverityText())
	assert.Equal(t, "test-log-int-0-4", split.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(4).SeverityText())
}

func TestSplitLogsPreserveSchemaURLOnPartialSplit(t *testing.T) {
	resourceSchemaURL := "https://test-resource-schema-url.com/"
	scopeSchemaURL := "https://test-scope-schema-url.com/"
	td := testdata.GenerateLogs(2)
	td.ResourceLogs().At(0).SetSchemaUrl(resourceSchemaURL)
	td.ResourceLogs().At(0).ScopeLogs().At(0).SetSchemaUrl(scopeSchemaURL)

	splitSize := 1
	split := splitLogs(splitSize, td)
	assert.Equal(t, resourceSchemaURL, split.ResourceLogs().At(0).SchemaUrl())
	assert.Equal(t, scopeSchemaURL, split.ResourceLogs().At(0).ScopeLogs().At(0).SchemaUrl())
}

func TestSplitLogsCappedPages(t *testing.T) {
	assertSplitLogsPages(t, "single-resource-single-scope", 1, 1, 7, 3)
}

func TestSplitLogsManyResourcesFallback(t *testing.T) {
	assertSplitLogsPages(t, "many-resources", 3, 1, 4, 5)
}

func TestSplitLogsManyScopesFallback(t *testing.T) {
	assertSplitLogsPages(t, "many-scopes", 1, 3, 4, 5)
}

func assertSplitLogsPages(t *testing.T, label string, resources, scopes, records, pageSize int) {
	t.Helper()

	source := newSplitLogsFixture(label, resources, scopes, records)
	remaining := resources * scopes * records
	actual := make([]plog.Logs, 0, (remaining+pageSize-1)/pageSize)
	for remaining > pageSize {
		actual = append(actual, splitLogs(pageSize, source))
		remaining -= pageSize
	}
	actual = append(actual, source)

	expected := expectedSplitLogsPages(label, resources, scopes, records, pageSize)
	require.Len(t, actual, len(expected))
	for pageIndex := range expected {
		require.Truef(t, pref.EqualLogs(expected[pageIndex], actual[pageIndex]), "page %d differs", pageIndex)
	}
}

func newSplitLogsFixture(label string, resources, scopes, records int) plog.Logs {
	page := newSplitLogsPage()
	for resourceIndex := range resources {
		for scopeIndex := range scopes {
			for recordIndex := range records {
				page.append(label, resourceIndex, scopeIndex, recordIndex)
			}
		}
	}
	return page.logs
}

func expectedSplitLogsPages(label string, resources, scopes, records, pageSize int) []plog.Logs {
	pages := make([]plog.Logs, 0, (resources*scopes*records+pageSize-1)/pageSize)
	var page *splitLogsPage
	pageRecords := pageSize
	for resourceIndex := range resources {
		for scopeIndex := range scopes {
			for recordIndex := range records {
				if pageRecords == pageSize {
					page = newSplitLogsPage()
					pages = append(pages, page.logs)
					pageRecords = 0
				}
				page.append(label, resourceIndex, scopeIndex, recordIndex)
				pageRecords++
			}
		}
	}
	return pages
}

type splitLogsPage struct {
	logs          plog.Logs
	resourceLogs  plog.ResourceLogs
	scopeLogs     plog.ScopeLogs
	resourceIndex int
	scopeIndex    int
}

func newSplitLogsPage() *splitLogsPage {
	return &splitLogsPage{
		logs:          plog.NewLogs(),
		resourceIndex: -1,
		scopeIndex:    -1,
	}
}

func (page *splitLogsPage) append(label string, resourceIndex, scopeIndex, recordIndex int) {
	if page.resourceIndex != resourceIndex {
		page.resourceLogs = page.logs.ResourceLogs().AppendEmpty()
		page.resourceLogs.Resource().Attributes().PutStr("resource.name", fmt.Sprintf("%s-resource-%d", label, resourceIndex))
		page.resourceLogs.Resource().Attributes().PutInt("resource.index", int64(resourceIndex))
		page.resourceLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/%s/resource/%d", label, resourceIndex))
		page.resourceIndex = resourceIndex
		page.scopeIndex = -1
	}
	if page.scopeIndex != scopeIndex {
		page.scopeLogs = page.resourceLogs.ScopeLogs().AppendEmpty()
		page.scopeLogs.Scope().SetName(fmt.Sprintf("%s-scope-%d", label, scopeIndex))
		page.scopeLogs.Scope().SetVersion(fmt.Sprintf("v%d", scopeIndex))
		page.scopeLogs.Scope().Attributes().PutStr("scope.name", fmt.Sprintf("%s-scope-%d", label, scopeIndex))
		page.scopeLogs.SetSchemaUrl(fmt.Sprintf("https://example.com/%s/scope/%d", label, scopeIndex))
		page.scopeIndex = scopeIndex
	}
	logRecord := page.scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetSeverityText(fmt.Sprintf("%s-resource-%d-scope-%d-record-%d", label, resourceIndex, scopeIndex, recordIndex))
	logRecord.Body().SetStr(fmt.Sprintf("%s-body-%d", label, recordIndex))
	logRecord.Attributes().PutInt("record.index", int64(recordIndex))
}

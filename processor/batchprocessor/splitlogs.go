// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor // import "go.opentelemetry.io/collector/processor/batchprocessor"

import (
	"go.opentelemetry.io/collector/pdata/plog"
)

// splitLogs removes logrecords from the input data and returns a new data of the specified size.
func splitLogs(size int, src plog.Logs) plog.Logs {
	if src.LogRecordCount() <= size {
		return src
	}
	return splitLogsWithKnownSize(size, src)
}

func splitLogsWithKnownSize(size int, src plog.Logs) plog.Logs {
	remaining := size
	dest := plog.NewLogs()

	for remaining > 0 {
		srcResourceLogs := src.ResourceLogs()
		srcResourceLog := srcResourceLogs.At(0)
		resourceLogCount := resourceLRC(srcResourceLog)
		if resourceLogCount <= remaining {
			remaining -= resourceLogCount
			moveFirstResourceLogs(srcResourceLogs, dest.ResourceLogs())
			continue
		}

		destResourceLog := dest.ResourceLogs().AppendEmpty()
		srcResourceLog.Resource().CopyTo(destResourceLog.Resource())
		destResourceLog.SetSchemaUrl(srcResourceLog.SchemaUrl())
		srcScopeLogs := srcResourceLog.ScopeLogs()
		for remaining > 0 {
			srcScopeLog := srcScopeLogs.At(0)
			logRecords := srcScopeLog.LogRecords()
			if logRecords.Len() <= remaining {
				remaining -= logRecords.Len()
				moveFirstScopeLogs(srcScopeLogs, destResourceLog.ScopeLogs())
				continue
			}

			destScopeLog := destResourceLog.ScopeLogs().AppendEmpty()
			srcScopeLog.Scope().CopyTo(destScopeLog.Scope())
			destScopeLog.SetSchemaUrl(srcScopeLog.SchemaUrl())
			logRecords.MoveFirstNTo(remaining, destScopeLog.LogRecords())
			remaining = 0
		}

		if srcScopeLogs.Len() == 0 {
			removeFirstResourceLogs(srcResourceLogs)
		}
	}

	return dest
}

func moveFirstResourceLogs(src, dest plog.ResourceLogsSlice) {
	moved := false
	src.RemoveIf(func(resourceLogs plog.ResourceLogs) bool {
		if moved {
			return false
		}
		resourceLogs.MoveTo(dest.AppendEmpty())
		moved = true
		return true
	})
}

func removeFirstResourceLogs(src plog.ResourceLogsSlice) {
	removed := false
	src.RemoveIf(func(plog.ResourceLogs) bool {
		if removed {
			return false
		}
		removed = true
		return true
	})
}

func moveFirstScopeLogs(src, dest plog.ScopeLogsSlice) {
	moved := false
	src.RemoveIf(func(scopeLogs plog.ScopeLogs) bool {
		if moved {
			return false
		}
		scopeLogs.MoveTo(dest.AppendEmpty())
		moved = true
		return true
	})
}

// resourceLRC calculates the total number of log records in the plog.ResourceLogs.
func resourceLRC(rs plog.ResourceLogs) (count int) {
	for k := 0; k < rs.ScopeLogs().Len(); k++ {
		count += rs.ScopeLogs().At(k).LogRecords().Len()
	}
	return count
}

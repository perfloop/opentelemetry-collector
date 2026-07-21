// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import "go.opentelemetry.io/collector/pdata/internal"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values and removes the transferred values from es.
// Moved values have the same ownership semantics as LogRecord.MoveTo.
// The source and destination slices must be distinct.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()

	dest.EnsureCapacity(dest.Len() + count)
	destRecords := *dest.orig
	destLen := len(destRecords)
	destRecords = destRecords[:destLen+count]
	srcRecords := *es.orig
	for i, srcRecord := range srcRecords[:count] {
		destRecord := internal.NewLogRecord()
		*destRecord, *srcRecord = *srcRecord, *destRecord
		destRecords[destLen+i] = destRecord
	}
	*dest.orig = destRecords
	clear(srcRecords[:count])
	*es.orig = srcRecords[count:]
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import "go.opentelemetry.io/collector/pdata/internal"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values and removes the transferred values from es.
// If es and dest designate the same slice, it does nothing.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	if count == 0 || es.orig == dest.orig {
		return
	}

	dest.EnsureCapacity(dest.Len() + count)
	destRecords := *dest.orig
	destLen := len(destRecords)
	destRecords = destRecords[:destLen+count]
	srcRecords := *es.orig
	for i, srcRecord := range srcRecords[:count] {
		// Keep retained source handles separate from the destination record.
		destRecord := internal.NewLogRecord()
		*destRecord, *srcRecord = *srcRecord, *destRecord
		destRecords[destLen+i] = destRecord
	}
	*dest.orig = destRecords
	clear(srcRecords[:count])
	*es.orig = srcRecords[count:]
}

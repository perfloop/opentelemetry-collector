// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import "go.opentelemetry.io/collector/pdata/internal"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values and removes the transferred values from es.
// count must be greater than zero and less than es.Len(). If es and dest designate
// the same slice, it does nothing.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	if es.orig == dest.orig {
		return
	}
	dest.EnsureCapacity(dest.Len() + count)
	destRecords := *dest.orig
	destLen := len(destRecords)
	destRecords = destRecords[:destLen+count]
	srcRecords := *es.orig
	for i, srcRecord := range srcRecords[:count] {
		// The fresh destination record needs no cleanup. Swapping its value keeps
		// srcRecord as a distinct, empty object for retained source accessors.
		destRecord := internal.NewLogRecord()
		*destRecord, *srcRecord = *srcRecord, *destRecord
		destRecords[destLen+i] = destRecord
	}
	*dest.orig = destRecords
	// Release moved records while the source retains the suffix's backing array.
	clear(srcRecords[:count])
	*es.orig = srcRecords[count:]
}

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import "go.opentelemetry.io/collector/pdata/internal"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values and removes the transferred values from es.
// Mutating source-derived handles after the move cannot change dest.
// The source and destination slices must be distinct.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	// A second prefix move must not eagerly detach pages from earlier moves.
	es.state.AssertMutableWithoutCallbacks()
	es.state.BeforeLogRecordSliceMove(es.orig)
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

	// movedRecords are the newly allocated destination holders. Retained source
	// LogRecord wrappers still point to the cleared source records and are never
	// entered in the destination replacement map.
	movedRecords := destRecords[destLen:]
	if es.state == dest.state {
		// A shared state cannot distinguish source mutation from destination use.
		// No caller can retain a wrapper for the newly allocated destination holder,
		// so it does not need a replacement mapping.
		copyMovedLogRecords(dest.state, dest.orig, movedRecords, false)
		return
	}

	// Handles retained from es still carry es.state even though their records
	// now belong to dest. Copy those records before a source mutation or a
	// later destination transfer can expose the shared data.
	transfer := &logRecordTransfer{state: dest.state, dest: dest.orig, records: movedRecords}
	es.state.OnMutation(transfer.detach)
	dest.state.OnLogRecordMove(dest.orig, transfer.detach)
}

type logRecordTransfer struct {
	state    *internal.State
	dest     *[]*internal.LogRecord
	records  []*internal.LogRecord
	detached bool
}

func (transfer *logRecordTransfer) detach() {
	if transfer.detached {
		return
	}
	transfer.detached = true
	copyMovedLogRecords(transfer.state, transfer.dest, transfer.records, true)
	transfer.state = nil
	transfer.dest = nil
	transfer.records = nil
}

func copyMovedLogRecords(state *internal.State, dest *[]*internal.LogRecord, movedRecords []*internal.LogRecord, replaceWrappers bool) {
	for _, movedRecord := range movedRecords {
		for index, destinationRecord := range *dest {
			if destinationRecord == movedRecord {
				replacement := copyLogRecord(movedRecord)
				(*dest)[index] = replacement
				if replaceWrappers {
					state.ReplaceLogRecord(movedRecord, replacement)
				}
			}
		}
	}
}

func copyLogRecord(src *internal.LogRecord) *internal.LogRecord {
	dest := internal.CopyLogRecord(nil, src)
	copyLogRecordBytes(dest, src)
	return dest
}

func copyLogRecordBytes(dest, src *internal.LogRecord) {
	copyAnyValueBytes(&dest.Body, &src.Body)
	for index := range src.Attributes {
		copyAnyValueBytes(&dest.Attributes[index].Value, &src.Attributes[index].Value)
	}
}

func copyAnyValueBytes(dest, src *internal.AnyValue) {
	switch srcValue := src.Value.(type) {
	case *internal.AnyValue_BytesValue:
		destValue := dest.Value.(*internal.AnyValue_BytesValue)
		destValue.BytesValue = cloneBytes(srcValue.BytesValue)
	case *internal.AnyValue_ArrayValue:
		if srcValue.ArrayValue == nil {
			return
		}
		destValue := dest.Value.(*internal.AnyValue_ArrayValue)
		for index := range srcValue.ArrayValue.Values {
			copyAnyValueBytes(&destValue.ArrayValue.Values[index], &srcValue.ArrayValue.Values[index])
		}
	case *internal.AnyValue_KvlistValue:
		if srcValue.KvlistValue == nil {
			return
		}
		destValue := dest.Value.(*internal.AnyValue_KvlistValue)
		for index := range srcValue.KvlistValue.Values {
			copyAnyValueBytes(&destValue.KvlistValue.Values[index].Value, &srcValue.KvlistValue.Values[index].Value)
		}
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}

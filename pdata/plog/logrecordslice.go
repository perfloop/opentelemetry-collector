// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

import "go.opentelemetry.io/collector/pdata/internal"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values, removes the transferred values from es, and
// does not share their mutable contents with source-derived handles.
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
		destRecords[destLen+i] = copyLogRecordForTransfer(srcRecord)
	}
	*dest.orig = destRecords
	clear(srcRecords[:count])
	*es.orig = srcRecords[count:]
}

func copyLogRecordForTransfer(src *internal.LogRecord) *internal.LogRecord {
	dest := internal.CopyLogRecord(nil, src)
	detachLogRecordBytes(dest, src)
	return dest
}

func detachLogRecordBytes(dest, src *internal.LogRecord) {
	detachAnyValueBytes(&dest.Body, &src.Body)
	for index := range src.Attributes {
		detachAnyValueBytes(&dest.Attributes[index].Value, &src.Attributes[index].Value)
	}
}

func detachAnyValueBytes(dest, src *internal.AnyValue) {
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
			detachAnyValueBytes(&destValue.ArrayValue.Values[index], &srcValue.ArrayValue.Values[index])
		}
	case *internal.AnyValue_KvlistValue:
		if srcValue.KvlistValue == nil {
			return
		}
		destValue := dest.Value.(*internal.AnyValue_KvlistValue)
		for index := range srcValue.KvlistValue.Values {
			detachAnyValueBytes(&destValue.KvlistValue.Values[index].Value, &srcValue.KvlistValue.Values[index].Value)
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

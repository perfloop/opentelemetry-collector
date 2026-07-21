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

	movedRecords := destRecords[destLen:]
	if es.state == dest.state {
		// A shared state cannot distinguish source mutation from destination use.
		// Copy the known appended range without registering a shared-state resolver.
		for index, movedRecord := range movedRecords {
			destRecords[destLen+index] = copyLogRecord(movedRecord)
		}
		return
	}

	// Handles retained from es carry es.state even though their nested values
	// now reside in dest. On the first source mutation or destination lifecycle
	// transition, redirect those source-state handles to isolated copies while
	// preserving the original destination record and its handles.
	transfer := &logRecordTransfer{sourceState: es.state, records: movedRecords}
	es.state.OnMutation(transfer.detach)
	dest.state.OnLogRecordMove(dest.orig, transfer.detach)
}

type logRecordTransfer struct {
	sourceState *internal.State
	records     []*internal.LogRecord
	detached    bool
}

func (transfer *logRecordTransfer) detach() {
	if transfer.detached {
		return
	}
	transfer.detached = true
	for _, record := range transfer.records {
		copySourceLogRecordValues(transfer.sourceState, record)
	}
	transfer.sourceState = nil
	transfer.records = nil
}

func copySourceLogRecordValues(state *internal.State, record *internal.LogRecord) {
	replacement := copyLogRecord(record)
	state.ReplaceKeyValueSlice(&record.Attributes, &replacement.Attributes)
	copySourceAnyValue(state, &record.Body, &replacement.Body)
	for index := range record.Attributes {
		copySourceAnyValue(state, &record.Attributes[index].Value, &replacement.Attributes[index].Value)
	}
}

func copySourceAnyValue(state *internal.State, value, replacement *internal.AnyValue) {
	state.ReplaceAnyValue(value, replacement)

	switch sourceValue := value.Value.(type) {
	case *internal.AnyValue_BytesValue:
		replacementValue, ok := replacement.Value.(*internal.AnyValue_BytesValue)
		if ok {
			state.ReplaceByteSlice(&sourceValue.BytesValue, &replacementValue.BytesValue)
		}
	case *internal.AnyValue_ArrayValue:
		if sourceValue.ArrayValue == nil {
			return
		}
		replacementValue, ok := replacement.Value.(*internal.AnyValue_ArrayValue)
		if !ok || replacementValue.ArrayValue == nil {
			return
		}
		state.ReplaceAnyValueSlice(&sourceValue.ArrayValue.Values, &replacementValue.ArrayValue.Values)
		for index := range sourceValue.ArrayValue.Values {
			copySourceAnyValue(state, &sourceValue.ArrayValue.Values[index], &replacementValue.ArrayValue.Values[index])
		}
	case *internal.AnyValue_KvlistValue:
		if sourceValue.KvlistValue == nil {
			return
		}
		replacementValue, ok := replacement.Value.(*internal.AnyValue_KvlistValue)
		if !ok || replacementValue.KvlistValue == nil {
			return
		}
		state.ReplaceKeyValueSlice(&sourceValue.KvlistValue.Values, &replacementValue.KvlistValue.Values)
		for index := range sourceValue.KvlistValue.Values {
			copySourceAnyValue(state, &sourceValue.KvlistValue.Values[index].Value, &replacementValue.KvlistValue.Values[index].Value)
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

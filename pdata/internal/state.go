// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "go.opentelemetry.io/collector/pdata/internal"
import (
	"sync/atomic"
)

// State defines an ownership state of pmetric.Metrics, plog.Logs, ptrace.Traces or pprofile.Profiles.
type State struct {
	onLogRecordMoveRecords         *[]*LogRecord
	onLogRecordMove                func()
	movedValues                    *movedValues
	onMutation                     []func()
	onAdditionalLogRecordMoveFuncs []logRecordMoveFunc
	refs                           atomic.Int32
	state                          uint32
}

type movedValues struct {
	anyValues      map[*AnyValue]*AnyValue
	byteSlices     map[*[]byte]*[]byte
	keyValueSlices map[*[]KeyValue]*[]KeyValue
	anyValueSlices map[*[]AnyValue]*[]AnyValue
}

type logRecordMoveFunc struct {
	records  *[]*LogRecord
	callback func()
}

const (
	defaultState          uint32 = 0
	stateReadOnlyBit             = uint32(1 << 0)
	statePipelineOwnedBit        = uint32(1 << 1)
)

func NewState() *State {
	st := &State{
		state: defaultState,
	}
	st.refs.Store(1)
	return st
}

func (st *State) MarkReadOnly() {
	st.runLogRecordMoveCallbacks()
	st.clearCallbacks()
	st.state |= stateReadOnlyBit
}

func (st *State) IsReadOnly() bool {
	return st.state&stateReadOnlyBit != 0
}

// AssertMutable panics if the state is not StateMutable.
func (st *State) AssertMutable() {
	st.AssertMutableWithoutCallbacks()
	callbacks := st.onMutation
	st.onMutation = nil
	for _, callback := range callbacks {
		callback()
	}
}

// AssertMutableWithoutCallbacks panics if the state is not StateMutable without
// running deferred ownership copies. It is for a transfer operation that extends
// the same source sequence without exposing a source mutation to callers.
func (st *State) AssertMutableWithoutCallbacks() {
	if st.state&stateReadOnlyBit != 0 {
		panic("invalid access to shared data")
	}
}

// OnMutation registers callback to run before the next mutable operation.
// Callbacks are dropped when the state is made read-only or released.
func (st *State) OnMutation(callback func()) {
	st.onMutation = append(st.onMutation, callback)
}

// OnLogRecordMove registers callback to run before records leave their slice.
func (st *State) OnLogRecordMove(records *[]*LogRecord, callback func()) {
	if st.onLogRecordMove == nil {
		st.onLogRecordMoveRecords = records
		st.onLogRecordMove = callback
		return
	}
	st.onAdditionalLogRecordMoveFuncs = append(st.onAdditionalLogRecordMoveFuncs, logRecordMoveFunc{records, callback})
}

// BeforeLogRecordSliceMove runs callbacks registered for records before they leave their slice.
func (st *State) BeforeLogRecordSliceMove(records *[]*LogRecord) {
	if st.onLogRecordMoveRecords == records {
		callback := st.onLogRecordMove
		st.onLogRecordMoveRecords = nil
		st.onLogRecordMove = nil
		callback()
	}

	callbacks := st.onAdditionalLogRecordMoveFuncs
	st.onAdditionalLogRecordMoveFuncs = st.onAdditionalLogRecordMoveFuncs[:0]
	for _, callback := range callbacks {
		if callback.records == records {
			callback.callback()
		} else {
			st.onAdditionalLogRecordMoveFuncs = append(st.onAdditionalLogRecordMoveFuncs, callback)
		}
	}
}

// BeforeLogRecordMove runs callbacks before an individual LogRecord moves.
func (st *State) BeforeLogRecordMove() {
	st.runLogRecordMoveCallbacks()
	st.onLogRecordMoveRecords = nil
	st.onLogRecordMove = nil
	st.onAdditionalLogRecordMoveFuncs = nil
}

func (st *State) sourceValues() *movedValues {
	if st.movedValues == nil {
		st.movedValues = &movedValues{}
	}
	return st.movedValues
}

// ReplaceAnyValue records the source-owned replacement for an AnyValue wrapper.
func (st *State) ReplaceAnyValue(old, replacement *AnyValue) {
	moved := st.sourceValues()
	if moved.anyValues == nil {
		moved.anyValues = make(map[*AnyValue]*AnyValue)
	}
	moved.anyValues[old] = replacement
}

// ResolveAnyValue returns the source-owned replacement for an AnyValue wrapper.
func (st *State) ResolveAnyValue(value *AnyValue) *AnyValue {
	if st.movedValues == nil {
		return value
	}
	for {
		replacement, ok := st.movedValues.anyValues[value]
		if !ok {
			return value
		}
		value = replacement
	}
}

// ReplaceByteSlice records the source-owned replacement for a ByteSlice wrapper.
func (st *State) ReplaceByteSlice(old, replacement *[]byte) {
	moved := st.sourceValues()
	if moved.byteSlices == nil {
		moved.byteSlices = make(map[*[]byte]*[]byte)
	}
	moved.byteSlices[old] = replacement
}

// ResolveByteSlice returns the source-owned replacement for a ByteSlice wrapper.
func (st *State) ResolveByteSlice(value *[]byte) *[]byte {
	if st.movedValues == nil {
		return value
	}
	for {
		replacement, ok := st.movedValues.byteSlices[value]
		if !ok {
			return value
		}
		value = replacement
	}
}

// ReplaceKeyValueSlice records the source-owned replacement for a Map wrapper.
func (st *State) ReplaceKeyValueSlice(old, replacement *[]KeyValue) {
	moved := st.sourceValues()
	if moved.keyValueSlices == nil {
		moved.keyValueSlices = make(map[*[]KeyValue]*[]KeyValue)
	}
	moved.keyValueSlices[old] = replacement
}

// ResolveKeyValueSlice returns the source-owned replacement for a Map wrapper.
func (st *State) ResolveKeyValueSlice(value *[]KeyValue) *[]KeyValue {
	if st.movedValues == nil {
		return value
	}
	for {
		replacement, ok := st.movedValues.keyValueSlices[value]
		if !ok {
			return value
		}
		value = replacement
	}
}

// ReplaceAnyValueSlice records the source-owned replacement for a Slice wrapper.
func (st *State) ReplaceAnyValueSlice(old, replacement *[]AnyValue) {
	moved := st.sourceValues()
	if moved.anyValueSlices == nil {
		moved.anyValueSlices = make(map[*[]AnyValue]*[]AnyValue)
	}
	moved.anyValueSlices[old] = replacement
}

// ResolveAnyValueSlice returns the source-owned replacement for a Slice wrapper.
func (st *State) ResolveAnyValueSlice(value *[]AnyValue) *[]AnyValue {
	if st.movedValues == nil {
		return value
	}
	for {
		replacement, ok := st.movedValues.anyValueSlices[value]
		if !ok {
			return value
		}
		value = replacement
	}
}

func (st *State) runLogRecordMoveCallbacks() {
	if st.onLogRecordMove != nil {
		st.onLogRecordMove()
	}
	for _, callback := range st.onAdditionalLogRecordMoveFuncs {
		callback.callback()
	}
}

func (st *State) clearCallbacks() {
	st.onMutation = nil
	st.onLogRecordMoveRecords = nil
	st.onLogRecordMove = nil
	st.onAdditionalLogRecordMoveFuncs = nil
}

// MarkPipelineOwned marks the data as owned by the pipeline, returns true if the data were
// previously not owned by the pipeline, otherwise false.
func (st *State) MarkPipelineOwned() bool {
	if st.state&statePipelineOwnedBit != 0 {
		return false
	}
	st.state |= statePipelineOwnedBit
	return true
}

// Ref add one to the count of active references.
func (st *State) Ref() {
	st.refs.Add(1)
}

// Unref returns true if reference count got to 0 which means no more active references,
// otherwise it returns false.
func (st *State) Unref() bool {
	refs := st.refs.Add(-1)
	switch {
	case refs > 0:
		return false
	case refs == 0:
		st.clearCallbacks()
		st.movedValues = nil
		return true
	default:
		panic("Cannot unref freed data")
	}
}

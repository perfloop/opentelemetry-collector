// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/pdata/internal"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	expected := generateTestLogRecordSlice()
	src := generateTestLogRecordSlice()
	dest := NewLogRecordSlice()

	src.MoveFirstNTo(2, dest)

	assert.Equal(t, expected.Len()-2, src.Len())
	assert.Equal(t, 2, dest.Len())
	assert.Equal(t, expected.At(0), dest.At(0))
	assert.Equal(t, expected.At(1), dest.At(1))
	assert.Equal(t, expected.At(2), src.At(0))
	assert.Equal(t, expected.At(3), src.At(1))

	src.MoveFirstNTo(src.Len(), dest)
	assert.Zero(t, src.Len())
	assert.Equal(t, expected, dest)
}

func TestLogRecordSliceMoveFirstNToReadOnly(t *testing.T) {
	readOnlyState := internal.NewState()
	readOnlyState.MarkReadOnly()
	readOnly := newLogRecordSlice(&[]*internal.LogRecord{}, readOnlyState)

	assert.Panics(t, func() { readOnly.MoveFirstNTo(0, NewLogRecordSlice()) })
	assert.Panics(t, func() { NewLogRecordSlice().MoveFirstNTo(0, readOnly) })
}

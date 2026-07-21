// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

// MoveFirstNTo appends the first count LogRecord values from es to dest in source order.
// It preserves existing dest values and removes the transferred values from es.
// count must be greater than zero and less than es.Len(). dest must not designate
// the same slice as es.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	*dest.orig = append(*dest.orig, (*es.orig)[:count]...)
	// Release moved records while the source retains the suffix's backing array.
	clear((*es.orig)[:count])
	*es.orig = (*es.orig)[count:]
}

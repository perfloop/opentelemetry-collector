// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog // import "go.opentelemetry.io/collector/pdata/plog"

// MoveFirstNTo moves at most count leading LogRecord values to dest.
// The source retains the remaining values in their original order.
func (es LogRecordSlice) MoveFirstNTo(count int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	if count <= 0 || es.orig == dest.orig {
		return
	}

	if count > len(*es.orig) {
		count = len(*es.orig)
	}
	dest.EnsureCapacity(dest.Len() + count)
	*dest.orig = append(*dest.orig, (*es.orig)[:count]...)
	clear((*es.orig)[:count])
	*es.orig = (*es.orig)[count:]
}

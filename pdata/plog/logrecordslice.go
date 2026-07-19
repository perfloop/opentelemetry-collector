// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

// MoveFirstNTo moves the first n log records to dest and removes them from es.
func (es LogRecordSlice) MoveFirstNTo(n int, dest LogRecordSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	if es.orig == dest.orig {
		return
	}
	moved := (*es.orig)[:n]
	*dest.orig = append(*dest.orig, moved...)
	clear(moved)
	*es.orig = (*es.orig)[n:]
}

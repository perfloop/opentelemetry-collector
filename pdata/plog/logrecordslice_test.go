// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package plog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogRecordSliceMoveFirstNTo(t *testing.T) {
	source := NewLogRecordSlice()
	source.AppendEmpty().SetSeverityText("first")
	source.AppendEmpty().SetSeverityText("second")
	source.AppendEmpty().SetSeverityText("third")
	destination := NewLogRecordSlice()
	destination.AppendEmpty().SetSeverityText("existing")

	source.MoveFirstNTo(2, destination)

	require.Equal(t, 1, source.Len())
	require.Equal(t, "third", source.At(0).SeverityText())
	require.Equal(t, 3, destination.Len())
	require.Equal(t, "existing", destination.At(0).SeverityText())
	require.Equal(t, "first", destination.At(1).SeverityText())
	require.Equal(t, "second", destination.At(2).SeverityText())
}

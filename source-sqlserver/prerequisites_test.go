package main

import (
	"context"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

func TestPrerequisites(t *testing.T) {
	// Table A exists and contains data, table B exists but is empty, and table C does not exist.
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = strings.ReplaceAll(tableA, "aaa", "ccc")
	tb.Insert(ctx, t, tableA, [][]any{{0, "hello"}, {1, "world"}})

	t.Run("validateAB", func(t *testing.T) {
		var cs = tb.CaptureSpec(t, tableA, tableB)
		_, err := cs.Validate(ctx, t)
		if err != nil {
			cupaloy.SnapshotT(t, err.Error())
		} else {
			cupaloy.SnapshotT(t, "no error")
		}
	})
	t.Run("validateABC-fails", func(t *testing.T) {
		var cs = tb.CaptureSpec(t, tableA, tableB, tableC)
		_, err := cs.Validate(ctx, t)
		if err != nil {
			cupaloy.SnapshotT(t, err.Error())
		} else {
			cupaloy.SnapshotT(t, "no error")
		}
	})
	t.Run("captureAB", func(t *testing.T) {
		var cs = tb.CaptureSpec(t, tableA, tableB)
		tests.VerifiedCapture(ctx, t, cs)
	})
	t.Run("captureABC-fails", func(t *testing.T) {
		var cs = tb.CaptureSpec(t, tableA, tableB, tableC)
		tests.VerifiedCapture(ctx, t, cs)
	})
}
package store

import (
	"testing"

	"dkv/internal/paxos"
)

func TestApplyPut(t *testing.T) {
	kv := NewKVStore(nil)

	op := &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "1", ClientID: "c1", OpID: 1}
	result, dup := kv.Apply(1, op)
	if dup {
		t.Fatal("should not be duplicate")
	}
	if result.Err != "" {
		t.Fatalf("unexpected error: %s", result.Err)
	}

	val, ok := kv.Get("x")
	if !ok || val != "1" {
		t.Fatalf("expected x=1, got %s (found=%v)", val, ok)
	}
}

func TestApplyAppend(t *testing.T) {
	kv := NewKVStore(nil)

	kv.Apply(1, &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "hello", ClientID: "c1", OpID: 1})
	kv.Apply(2, &paxos.Operation{Type: paxos.OpAppend, Key: "x", Value: "_world", ClientID: "c1", OpID: 2})

	val, ok := kv.Get("x")
	if !ok || val != "hello_world" {
		t.Fatalf("expected hello_world, got %s", val)
	}
}

func TestApplyDelete(t *testing.T) {
	kv := NewKVStore(nil)

	kv.Apply(1, &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "1", ClientID: "c1", OpID: 1})
	result, _ := kv.Apply(2, &paxos.Operation{Type: paxos.OpDelete, Key: "x", ClientID: "c1", OpID: 2})
	if !result.Found {
		t.Fatal("delete should report found=true")
	}

	_, ok := kv.Get("x")
	if ok {
		t.Fatal("key should be deleted")
	}
}

func TestApplyNoop(t *testing.T) {
	kv := NewKVStore(nil)

	kv.Apply(1, &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "1", ClientID: "c1", OpID: 1})
	_, dup := kv.Apply(2, &paxos.Operation{Type: paxos.OpNoop})
	if dup {
		t.Fatal("noop should not be duplicate")
	}

	val, ok := kv.Get("x")
	if !ok || val != "1" {
		t.Fatal("noop should not change state")
	}
}

func TestDuplicateSuppression(t *testing.T) {
	kv := NewKVStore(nil)

	op := &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "1", ClientID: "c1", OpID: 1}
	_, dup1 := kv.Apply(1, op)
	if dup1 {
		t.Fatal("first apply should not be duplicate")
	}

	// Same clientID and opID but at a different log index — duplicate.
	_, dup2 := kv.Apply(2, op)
	if !dup2 {
		t.Fatal("second apply with same opID should be duplicate")
	}

	// Higher opID — not duplicate.
	op2 := &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "2", ClientID: "c1", OpID: 2}
	_, dup3 := kv.Apply(3, op2)
	if dup3 {
		t.Fatal("higher opID should not be duplicate")
	}
}

func TestCheckDuplicate(t *testing.T) {
	kv := NewKVStore(nil)

	// No entries yet.
	isDup, _ := kv.CheckDuplicate("c1", 1)
	if isDup {
		t.Fatal("should not be duplicate before any applies")
	}

	kv.Apply(1, &paxos.Operation{Type: paxos.OpPut, Key: "x", Value: "v", ClientID: "c1", OpID: 5})

	isDup, _ = kv.CheckDuplicate("c1", 5)
	if !isDup {
		t.Fatal("opID 5 should be duplicate")
	}

	isDup, _ = kv.CheckDuplicate("c1", 3)
	if !isDup {
		t.Fatal("opID 3 (< 5) should be duplicate")
	}

	isDup, _ = kv.CheckDuplicate("c1", 6)
	if isDup {
		t.Fatal("opID 6 should not be duplicate")
	}
}

func TestLastAppliedIndex(t *testing.T) {
	kv := NewKVStore(nil)

	if kv.LastAppliedIndex() != 0 {
		t.Fatal("initial lastApplied should be 0")
	}

	kv.Apply(1, &paxos.Operation{Type: paxos.OpNoop})
	if kv.LastAppliedIndex() != 1 {
		t.Fatalf("expected 1, got %d", kv.LastAppliedIndex())
	}

	kv.Apply(2, &paxos.Operation{Type: paxos.OpPut, Key: "k", Value: "v", ClientID: "c", OpID: 1})
	if kv.LastAppliedIndex() != 2 {
		t.Fatalf("expected 2, got %d", kv.LastAppliedIndex())
	}
}

func TestSnapshot(t *testing.T) {
	kv := NewKVStore(nil)

	kv.Apply(1, &paxos.Operation{Type: paxos.OpPut, Key: "a", Value: "1", ClientID: "c", OpID: 1})
	kv.Apply(2, &paxos.Operation{Type: paxos.OpPut, Key: "b", Value: "2", ClientID: "c", OpID: 2})

	snap := kv.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap))
	}
	if snap["a"] != "1" || snap["b"] != "2" {
		t.Fatalf("wrong snapshot: %v", snap)
	}
}

package series_test

import (
	"sync"
	"testing"

	"github.com/dreamsxin/gota/v2/series"
)

// Tests for the Milestone 3 chain-local intern pool (RFC §9.2). The pool is
// lock-free by contract: one context per transformation chain.

func TestExecutionContext_Intern(t *testing.T) {
	ctx := series.NewExecutionContext()
	defer ctx.Release()

	a := ctx.Intern("group-key-1")
	b := ctx.Intern("group-key-1")
	if a != b {
		t.Fatal("interned strings must be the same canonical value")
	}
	// Same content from a different backing array returns the canonical copy.
	other := string([]byte("group-key-1"))
	if got := ctx.Intern(other); got != a {
		t.Fatal("intern must deduplicate equal strings")
	}
	if ctx.Len() != 1 {
		t.Fatalf("Len = %d, want 1", ctx.Len())
	}
	ctx.Intern("group-key-2")
	if ctx.Len() != 2 {
		t.Fatalf("Len = %d, want 2", ctx.Len())
	}
}

func TestExecutionContext_Release(t *testing.T) {
	ctx := series.NewExecutionContext()
	ctx.Intern("x")
	ctx.Release()
	if ctx.Len() != 0 {
		t.Fatalf("released context Len = %d, want 0", ctx.Len())
	}
	// Intern after release degrades to a pass-through.
	if got := ctx.Intern("y"); got != "y" {
		t.Fatalf("pass-through failed: %q", got)
	}
}

func TestExecutionContext_NilIsNoOp(t *testing.T) {
	var ctx *series.ExecutionContext
	if got := ctx.Intern("z"); got != "z" {
		t.Fatalf("nil context must pass through: %q", got)
	}
	if ctx.Len() != 0 {
		t.Fatal("nil context Len must be 0")
	}
	ctx.Release() // must not panic
}

// TestExecutionContext_ConcurrentIsolation runs one context per goroutine,
// mirroring CapplyParallel-style concurrency: separate chains own separate
// pools, so no locking is required and results stay isolated.
func TestExecutionContext_ConcurrentIsolation(t *testing.T) {
	const workers = 8
	const perWorker = 1000
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := series.NewExecutionContext()
			defer ctx.Release()
			prefix := string(rune('a' + id))
			for i := 0; i < perWorker; i++ {
				key := prefix + "-key"
				if got := ctx.Intern(key); got != key {
					errs <- errIsolation
					return
				}
			}
			if ctx.Len() != 1 {
				errs <- errIsolation
				return
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

var errIsolation = errSentinel("context isolation violated")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

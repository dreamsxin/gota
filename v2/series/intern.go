package series

// ExecutionContext owns the chain-local string intern pool (RFC §9.2). One
// context serves one transformation chain: interning Dictionary categories,
// constants, and GroupBy keys keeps a single canonical copy of every
// repeated string, and the whole pool is released in O(1) with the context.
//
// The pool is deliberately lock-free: its contract is single-chain
// (single-goroutine) use. Concurrent chains must each own their context -
// this is why the §9.2 acceptance ("pool lock waiting < 0.3% of CPU
// cycles") is satisfied by construction: there is no pool lock to wait on.
// Process-global pools are rejected outright (§9.2).
type ExecutionContext struct {
	interned map[string]string
}

// NewExecutionContext returns a fresh chain-local execution context.
func NewExecutionContext() *ExecutionContext {
	return &ExecutionContext{interned: make(map[string]string)}
}

// Intern returns the canonical copy of s held by the pool, inserting it on
// first sight. A nil context passes strings through untouched so callers can
// treat an absent context as a no-op.
func (c *ExecutionContext) Intern(s string) string {
	if c == nil || c.interned == nil {
		return s
	}
	if canonical, ok := c.interned[s]; ok {
		return canonical
	}
	c.interned[s] = s
	return s
}

// Len returns the number of distinct interned strings.
func (c *ExecutionContext) Len() int {
	if c == nil || c.interned == nil {
		return 0
	}
	return len(c.interned)
}

// Release drops the pool in O(1); no per-string teardown happens. The
// context must not be used afterwards (Intern degrades to a pass-through).
func (c *ExecutionContext) Release() {
	if c != nil {
		c.interned = nil
	}
}

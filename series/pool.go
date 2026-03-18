package series

import (
	"sync"
)

// Pool for reusing Series objects to reduce GC pressure
var (
	// seriesPool caches Series objects for reuse
	seriesPool = sync.Pool{
		New: func() interface{} {
			return &Series{
				elements: nil,
				t:        String,
				Name:     "",
			}
		},
	}

	// elementPools caches element slices for different types
	intElementPool    = sync.Pool{New: func() interface{} { return make([]intElement, 0, 1024) }}
	floatElementPool  = sync.Pool{New: func() interface{} { return make([]floatElement, 0, 1024) }}
	stringElementPool = sync.Pool{New: func() interface{} { return make([]stringElement, 0, 1024) }}
	boolElementPool   = sync.Pool{New: func() interface{} { return make([]boolElement, 0, 1024) }}
	timeElementPool   = sync.Pool{New: func() interface{} { return make([]timeElement, 0, 1024) }}
)

// GetSeries retrieves a Series from the pool
func GetSeries() *Series {
	return seriesPool.Get().(*Series)
}

// PutSeries returns a Series to the pool after resetting it
func PutSeries(s *Series) {
	s.elements = nil
	s.t = String
	s.Name = ""
	s.Err = nil
	seriesPool.Put(s)
}

// GetIntElements retrieves an int element slice from the pool
func GetIntElements(capacity int) []intElement {
	pool := intElementPool.Get().([]intElement)
	if cap(pool) >= capacity {
		return pool[:capacity]
	}
	// Pool capacity insufficient, allocate new
	return make([]intElement, capacity)
}

// PutIntElements returns an int element slice to the pool
func PutIntElements(elems []intElement) {
	intElementPool.Put(elems[:0])
}

// GetFloatElements retrieves a float element slice from the pool
func GetFloatElements(capacity int) []floatElement {
	pool := floatElementPool.Get().([]floatElement)
	if cap(pool) >= capacity {
		return pool[:capacity]
	}
	return make([]floatElement, capacity)
}

// PutFloatElements returns a float element slice to the pool
func PutFloatElements(elems []floatElement) {
	floatElementPool.Put(elems[:0])
}

// GetStringElements retrieves a string element slice from the pool
func GetStringElements(capacity int) []stringElement {
	pool := stringElementPool.Get().([]stringElement)
	if cap(pool) >= capacity {
		return pool[:capacity]
	}
	return make([]stringElement, capacity)
}

// PutStringElements returns a string element slice to the pool
func PutStringElements(elems []stringElement) {
	stringElementPool.Put(elems[:0])
}

// GetBoolElements retrieves a bool element slice from the pool
func GetBoolElements(capacity int) []boolElement {
	pool := boolElementPool.Get().([]boolElement)
	if cap(pool) >= capacity {
		return pool[:capacity]
	}
	return make([]boolElement, capacity)
}

// PutBoolElements returns a bool element slice to the pool
func PutBoolElements(elems []boolElement) {
	boolElementPool.Put(elems[:0])
}

// GetTimeElements retrieves a time element slice from the pool
func GetTimeElements(capacity int) []timeElement {
	pool := timeElementPool.Get().([]timeElement)
	if cap(pool) >= capacity {
		return pool[:capacity]
	}
	return make([]timeElement, capacity)
}

// PutTimeElements returns a time element slice to the pool
func PutTimeElements(elems []timeElement) {
	timeElementPool.Put(elems[:0])
}

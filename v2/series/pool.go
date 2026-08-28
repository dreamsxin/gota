package series

import (
	"sync"
)

// seriesPool caches Series objects for reuse to reduce GC pressure.
// Use GetSeries / PutSeries for safe access.
var seriesPool = sync.Pool{
	New: func() interface{} {
		return &Series{
			elements: nil,
			t:        String,
			Name:     "",
		}
	},
}

// GetSeries retrieves a Series from the pool.
// Callers must call PutSeries when done.
func GetSeries() *Series {
	return seriesPool.Get().(*Series)
}

// PutSeries returns a Series to the pool after resetting it.
func PutSeries(s *Series) {
	s.elements = nil
	s.t = String
	s.Name = ""
	s.Err = nil
	seriesPool.Put(s)
}

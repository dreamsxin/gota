package series

import "math"

// EWM holds parameters for Exponentially Weighted calculations, mirroring
// pandas ewm().  Exactly one of Alpha/Span/HalfLife/COM must be non-zero.
type EWM struct {
	series     Series
	alpha      float64 // smoothing factor in (0,1]
	adjust     bool    // pandas adjust=True: use expanding weights (default)
	ignoreNA   bool    // skip NaN in weight calculation
	minPeriods int     // minimum observations to produce a result
}

// EWM creates a new EWM object.  span is the most common parameter (like pandas
// ewm(span=N)).  alpha = 2/(span+1).  Use span >= 1.
func (s Series) EWM(span float64) EWM {
	alpha := 2.0 / (span + 1.0)
	return EWM{series: s, alpha: alpha, adjust: true, ignoreNA: false, minPeriods: 0}
}

// EWMAlpha creates an EWM directly from an alpha value in (0,1].
func (s Series) EWMAlpha(alpha float64) EWM {
	return EWM{series: s, alpha: alpha, adjust: true, ignoreNA: false, minPeriods: 0}
}

// MinPeriods sets the minimum observations required.
func (e EWM) MinPeriods(n int) EWM { e.minPeriods = n; return e }

// Adjust sets the adjust flag (pandas adjust parameter).
func (e EWM) Adjust(a bool) EWM { e.adjust = a; return e }

// IgnoreNA sets whether NaN values should be ignored in weight calculation.
func (e EWM) IgnoreNA(v bool) EWM { e.ignoreNA = v; return e }

// Mean returns the exponentially weighted moving average (EWMA).
func (e EWM) Mean() Series {
	s := New([]float64{}, Float, "EWM_mean")
	vals := make([]float64, e.series.Len())
	for i := 0; i < e.series.Len(); i++ {
		elem := e.series.Elem(i)
		if elem.IsNA() {
			vals[i] = math.NaN()
		} else {
			vals[i] = elem.Float()
		}
	}
	n := len(vals)
	alpha := e.alpha
	result := make([]float64, n)

	if e.adjust {
		// adjusted (pandas-compatible):
		//   weight for position j (0=oldest in window) = (1-alpha)^(i-j)
		//   result[i] = sum_{j=0}^{i} w_j * x_j / sum_{j=0}^{i} w_j
		// We iterate from oldest (j=0) to newest (j=i); w grows by /= (1-alpha)
		// each step so that the newest element always has weight 1.
		for i := 0; i < n; i++ {
			num, den := 0.0, 0.0
			validCount := 0
			// w[j] = (1-alpha)^(i-j), so starting from j=0: w=(1-alpha)^i,
			// then for j=1: w=(1-alpha)^(i-1), ..., j=i: w=1.
			w := math.Pow(1-alpha, float64(i))
			for j := 0; j <= i; j++ {
				if !math.IsNaN(vals[j]) {
					num += w * vals[j]
					den += w
					validCount++
				}
				// Move weight to next (newer) position: divide by (1-alpha).
				if j < i {
					if (1 - alpha) > 0 {
						w /= (1 - alpha)
					}
				}
			}
			if validCount < e.minPeriods {
				result[i] = math.NaN()
			} else if den == 0 {
				result[i] = math.NaN()
			} else {
				result[i] = num / den
			}
		}
	} else {
		// non-adjusted (recursive): ewma[i] = alpha*x[i] + (1-alpha)*ewma[i-1]
		ewma := math.NaN()
		validCount := 0
		for i := 0; i < n; i++ {
			if math.IsNaN(vals[i]) {
				if !e.ignoreNA {
					ewma = math.NaN()
				}
				if validCount < e.minPeriods {
					result[i] = math.NaN()
				} else {
					result[i] = ewma
				}
			} else {
				validCount++
				if math.IsNaN(ewma) {
					ewma = vals[i]
				} else {
					ewma = alpha*vals[i] + (1-alpha)*ewma
				}
				if validCount < e.minPeriods {
					result[i] = math.NaN()
				} else {
					result[i] = ewma
				}
			}
		}
	}
	for _, v := range result {
		s.Append(v)
	}
	return s
}

// Var returns the exponentially weighted variance (ddof=1).
// Uses the pandas-compatible formula:
//
//	V[i] = Σ w_j*(x_j - μ_i)² / (Σ w_j - Σ w_j²/Σ w_j)
//
// where weights w_j = (1-α)^(i-j) (adjusted mode).
func (e EWM) Var() Series {
	s := New([]float64{}, Float, "EWM_var")
	vals := make([]float64, e.series.Len())
	for i := 0; i < e.series.Len(); i++ {
		elem := e.series.Elem(i)
		if elem.IsNA() {
			vals[i] = math.NaN()
		} else {
			vals[i] = elem.Float()
		}
	}
	alpha := e.alpha
	n := len(vals)

	for i := 0; i < n; i++ {
		// Accumulate weighted mean, sum-of-weights, sum-of-squared-weights,
		// and weighted sum-of-squared-deviations in a single forward pass.
		var sumW, sumW2, mean, m2 float64
		validCount := 0

		// w[j] = (1-α)^(i-j); newest element (j=i) has weight 1.
		w := math.Pow(1-alpha, float64(i))
		for j := 0; j <= i; j++ {
			if !math.IsNaN(vals[j]) {
				validCount++
				sumW += w
				sumW2 += w * w
				// Welford-style online update for weighted mean and M2.
				oldMean := mean
				mean += (w / sumW) * (vals[j] - oldMean)
				m2 += w * (vals[j] - oldMean) * (vals[j] - mean)
			}
			if j < i && (1-alpha) > 0 {
				w /= (1 - alpha)
			}
		}

		if validCount < e.minPeriods || validCount < 2 {
			s.Append(math.NaN())
			continue
		}
		// Bessel correction for weighted variance (pandas formula):
		// var = M2 / (sumW - sumW2/sumW)
		denom := sumW - sumW2/sumW
		if denom <= 0 {
			s.Append(math.NaN())
		} else {
			s.Append(m2 / denom)
		}
	}
	return s
}

// Std returns the exponentially weighted standard deviation (sqrt of EWM Var).
func (e EWM) Std() Series {
	v := e.Var()
	s := New([]float64{}, Float, "EWM_std")
	for i := 0; i < v.Len(); i++ {
		elem := v.Elem(i)
		if elem.IsNA() {
			s.Append(math.NaN())
		} else {
			s.Append(math.Sqrt(elem.Float()))
		}
	}
	return s
}

// RollingWindow is used for rolling window calculations.
type RollingWindow struct {
	window     int
	minPeriods int // minimum number of non-NaN observations required; 0 means use window size
	series     Series
}

// Rolling creates a new RollingWindow with the given window size.
// By default minPeriods equals the window size.
func (s Series) Rolling(window int) RollingWindow {
	return RollingWindow{
		window:     window,
		minPeriods: window,
		series:     s,
	}
}

// MinPeriods sets the minimum number of non-NaN observations required to
// produce a result.  Returns the RollingWindow for chaining.
func (r RollingWindow) MinPeriods(n int) RollingWindow {
	r.minPeriods = n
	return r
}

// floatSlice extracts the float64 values of the series once, replacing NaN
// elements with math.NaN().
func (r RollingWindow) floatSlice() []float64 {
	n := r.series.Len()
	vals := make([]float64, n)
	for i := 0; i < n; i++ {
		elem := r.series.Elem(i)
		if elem.IsNA() {
			vals[i] = math.NaN()
		} else {
			vals[i] = elem.Float()
		}
	}
	return vals
}

// nanResult returns a NaN float64 Series of length n with the given name.
func nanResult(name string) Series {
	return New([]float64{}, Float, name)
}

// hasEnough returns true if the window [start,end) has at least minPeriods
// non-NaN values.
func hasEnough(vals []float64, start, end, minPeriods int) bool {
	count := 0
	for i := start; i < end; i++ {
		if !math.IsNaN(vals[i]) {
			count++
		}
	}
	return count >= minPeriods
}

// Mean returns the rolling mean using an O(n) sliding-sum algorithm.
func (r RollingWindow) Mean() Series {
	s := New([]float64{}, Float, "Mean")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	var windowSum float64
	var windowCount int

	for i := 0; i < n; i++ {
		// Add incoming element.
		if !math.IsNaN(vals[i]) {
			windowSum += vals[i]
			windowCount++
		}
		// Remove outgoing element (the one falling off the left).
		if i >= w {
			out := vals[i-w]
			if !math.IsNaN(out) {
				windowSum -= out
				windowCount--
			}
		}
		// Output NaN if window not yet full (respects minPeriods).
		if windowCount < r.minPeriods {
			s.Append(math.NaN())
		} else {
			s.Append(windowSum / float64(windowCount))
		}
	}
	return s
}

// Sum returns the rolling sum using an O(n) sliding algorithm.
func (r RollingWindow) Sum() Series {
	s := New([]float64{}, Float, "Sum")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	var windowSum float64
	var windowCount int

	for i := 0; i < n; i++ {
		if !math.IsNaN(vals[i]) {
			windowSum += vals[i]
			windowCount++
		}
		if i >= w {
			out := vals[i-w]
			if !math.IsNaN(out) {
				windowSum -= out
				windowCount--
			}
		}
		if windowCount < r.minPeriods {
			s.Append(math.NaN())
		} else {
			s.Append(windowSum)
		}
	}
	return s
}

// Min returns the rolling minimum.
// Uses a deque-based O(n) monotonic queue algorithm.
func (r RollingWindow) Min() Series {
	s := New([]float64{}, Float, "Min")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	// deque stores indices; front is always the index of the current window min.
	deque := make([]int, 0, w)

	for i := 0; i < n; i++ {
		// Remove indices outside the window.
		for len(deque) > 0 && deque[0] <= i-w {
			deque = deque[1:]
		}
		// Maintain ascending order at the back (skip NaN — treat as +Inf).
		v := vals[i]
		for len(deque) > 0 {
			dv := vals[deque[len(deque)-1]]
			if math.IsNaN(dv) || (!math.IsNaN(v) && dv >= v) {
				deque = deque[:len(deque)-1]
			} else {
				break
			}
		}
		deque = append(deque, i)

		start := i - w + 1
		if start < 0 {
			start = 0
		}
		if !hasEnough(vals, start, i+1, r.minPeriods) {
			s.Append(math.NaN())
		} else {
			s.Append(vals[deque[0]])
		}
	}
	return s
}

// Max returns the rolling maximum.
// Uses a deque-based O(n) monotonic queue algorithm.
func (r RollingWindow) Max() Series {
	s := New([]float64{}, Float, "Max")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	deque := make([]int, 0, w)

	for i := 0; i < n; i++ {
		for len(deque) > 0 && deque[0] <= i-w {
			deque = deque[1:]
		}
		v := vals[i]
		for len(deque) > 0 {
			dv := vals[deque[len(deque)-1]]
			if math.IsNaN(dv) || (!math.IsNaN(v) && dv <= v) {
				deque = deque[:len(deque)-1]
			} else {
				break
			}
		}
		deque = append(deque, i)

		start := i - w + 1
		if start < 0 {
			start = 0
		}
		if !hasEnough(vals, start, i+1, r.minPeriods) {
			s.Append(math.NaN())
		} else {
			s.Append(vals[deque[0]])
		}
	}
	return s
}

// StdDev returns the rolling standard deviation (Bessel-corrected, ddof=1).
// Uses Welford's online algorithm adapted for a sliding window: O(n).
func (r RollingWindow) StdDev() Series {
	s := New([]float64{}, Float, "StdDev")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	// Welford's online algorithm for a sliding window.
	// We maintain count, mean, and M2 (sum of squared deviations from mean).
	// When a value enters the window: update mean and M2.
	// When a value leaves the window: downdate mean and M2.
	var count int
	var mean, m2 float64

	for i := 0; i < n; i++ {
		// Add incoming element.
		if !math.IsNaN(vals[i]) {
			count++
			delta := vals[i] - mean
			mean += delta / float64(count)
			delta2 := vals[i] - mean
			m2 += delta * delta2
		}

		// Remove outgoing element (falls off the left of the window).
		if i >= w {
			out := vals[i-w]
			if !math.IsNaN(out) {
				count--
				if count == 0 {
					mean = 0
					m2 = 0
				} else {
					delta := out - mean
					mean -= delta / float64(count)
					delta2 := out - mean
					m2 -= delta * delta2
					if m2 < 1e-14 {
						m2 = 0 // guard against floating-point underflow
					}
				}
			}
		}

		if count < r.minPeriods || count < 2 {
			s.Append(math.NaN())
		} else {
			s.Append(math.Sqrt(m2 / float64(count-1)))
		}
	}
	return s
}

// Apply applies a user-supplied function to each rolling window and returns
// the results as a Float Series.  The function receives a slice of the
// non-NaN float64 values in the current window.  It should return math.NaN()
// when the window is insufficient.
func (r RollingWindow) Apply(f func([]float64) float64) Series {
	s := New([]float64{}, Float, "Apply")
	vals := r.floatSlice()
	n := len(vals)
	w := r.window

	for i := 0; i < n; i++ {
		start := i - w + 1
		if start < 0 {
			start = 0
		}
		var wvals []float64
		for j := start; j <= i; j++ {
			if !math.IsNaN(vals[j]) {
				wvals = append(wvals, vals[j])
			}
		}
		if len(wvals) < r.minPeriods {
			s.Append(math.NaN())
		} else {
			s.Append(f(wvals))
		}
	}
	return s
}

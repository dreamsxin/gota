package series

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"
)

// Series is a data structure designed for operating on arrays of elements that
// should comply with a certain type structure. They are flexible enough that
// can be transformed to other Series types and account for missing or non
// valid elements. Most of the power of Series resides on the ability to
// compare and subset Series of different types.
//
// Storage is a contiguous typed column buffer with a validity bitmap; see
// docs/rfc-columnar-kernel.md. Missing values are tracked by the bitmap,
// never by sentinel values.
type Series struct {
	Name     string // The name of the series
	elements store  // The column buffer holding the values
	t        Type   // The type of the series

	// deprecated: use Error() instead
	Err error
}

// compFunc defines a user-defined comparator function over a Series row.
// Used internally for type assertions.
type compFunc = func(s Series, i int) bool

// Comparator is a convenience alias that can be used for a more type safe way of
// reason and use comparators.
type Comparator string

// Supported Comparators
const (
	Eq        Comparator = "=="   // Equal
	Neq       Comparator = "!="   // Non equal
	Greater   Comparator = ">"    // Greater than
	GreaterEq Comparator = ">="   // Greater or equal than
	Less      Comparator = "<"    // Lesser than
	LessEq    Comparator = "<="   // Lesser or equal than
	In        Comparator = "in"   // Inside
	Out       Comparator = "out"  // Outside
	CompFunc  Comparator = "func" // user-defined comparison function
)

// Type is a convenience alias that can be used for a more type safe way of
// reason and use Series types.
type Type string

// Supported Series Types
const (
	String Type = "string"
	Int    Type = "int"
	Float  Type = "float"
	Bool   Type = "bool"
	Time   Type = "time"
)

// Indexes represent the elements that can be used for selecting a subset of
// elements within a Series. Currently supported are:
//
//	int            // Matches the given index number
//	[]int          // Matches all given index numbers
//	[]bool         // Matches all elements in a Series marked as true
//	Series [Int]   // Same as []int
//	Series [Bool]  // Same as []bool
type Indexes interface{}

// emptyStore returns an empty buffer of the given type with the requested
// capacity.
func emptyStore(t Type, capacity int) store {
	if capacity < 0 {
		capacity = 0
	}
	switch t {
	case String:
		return newColumnCap[string](capacity)
	case Int:
		return newColumnCap[int64](capacity)
	case Float:
		return newColumnCap[float64](capacity)
	case Bool:
		return newColumnCap[bool](capacity)
	case Time:
		return newColumnCap[time.Time](capacity)
	default:
		panic(fmt.Sprintf("unknown type %v", t))
	}
}

// New is the generic Series constructor
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate a zeroed buffer of the requested length.
	preAlloc := func(n int) {
		switch t {
		case String:
			ret.elements = newColumn[string](n)
		case Int:
			ret.elements = newColumn[int64](n)
		case Float:
			ret.elements = newColumn[float64](n)
		case Bool:
			ret.elements = newColumn[bool](n)
		case Time:
			ret.elements = newColumn[time.Time](n)
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}

	if values == nil {
		preAlloc(1)
		setAt(&ret.elements, t, 0, nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		if ret, ok := stringsToSeriesDirect(v, t, name); ok {
			return ret
		}
		ret.elements = emptyStore(t, 0)
		for _, sv := range v {
			appendAs(&ret.elements, t, sv)
		}
	case []float64:
		if t == Float {
			ret = FloatsDirect(v)
			ret.Name = name
			return ret
		}
		ret.elements = emptyStore(t, 0)
		for _, fv := range v {
			appendAs(&ret.elements, t, fv)
		}
	case []int:
		switch t {
		case Int:
			ret = IntsDirect(v)
			ret.Name = name
			return ret
		case Float:
			return BatchConvertInts(v, Float, name)
		}
		ret.elements = emptyStore(t, 0)
		for _, iv := range v {
			appendAs(&ret.elements, t, iv)
		}
	case []bool:
		if t == Bool {
			ret = BoolsDirect(v)
			ret.Name = name
			return ret
		}
		ret.elements = emptyStore(t, 0)
		for _, bv := range v {
			appendAs(&ret.elements, t, bv)
		}
	case []time.Time:
		if t == Time {
			ret = TimesDirect(v)
			ret.Name = name
			return ret
		}
		ret.elements = emptyStore(t, 0)
		for _, tv := range v {
			appendAs(&ret.elements, t, tv)
		}
	case Series:
		ret.elements = emptyStore(t, 0)
		for i := 0; i < v.Len(); i++ {
			if v.IsNA(i) {
				appendAs(&ret.elements, t, "NaN")
			} else {
				appendAs(&ret.elements, t, v.Val(i))
			}
		}
	default:
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			rv := reflect.ValueOf(values)
			l := rv.Len()
			ret.elements = emptyStore(t, 0)
			for i := 0; i < l; i++ {
				appendAs(&ret.elements, t, rv.Index(i).Interface())
			}
		default:
			ret.elements = emptyStore(t, 0)
			appendAs(&ret.elements, t, reflect.ValueOf(values).Interface())
		}
	}

	return ret
}

// Strings is a constructor for a String Series
func Strings(values interface{}) Series {
	return New(values, String, "")
}

// Ints is a constructor for an Int Series
func Ints(values interface{}) Series {
	return New(values, Int, "")
}

// Floats is a constructor for a Float Series
func Floats(values interface{}) Series {
	return New(values, Float, "")
}

// Bools is a constructor for a Bool Series
func Bools(values interface{}) Series {
	return New(values, Bool, "")
}

// Times is a constructor for a Time Series
func Times(values interface{}) Series {
	return New(values, Time, "")
}

// Empty returns an empty Series of the same type
func (s Series) Empty() Series {
	return s.EmptyWithCapacity(0)
}

// EmptyWithCapacity returns an empty Series of the same type with enough
// capacity for callers that know the result size up front.
func (s Series) EmptyWithCapacity(capacity int) Series {
	return Series{Name: s.Name, t: s.t, elements: emptyStore(s.t, capacity)}
}

// Returns Error or nil if no error occured
func (s *Series) Error() error {
	return s.Err
}

func (s *Series) Fill(num int, values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	news := New(values, s.t, s.Name)
	for i := s.Len(); i < num; i++ {
		s.elements = s.elements.appendStore(news.elements)
	}
}

// Append adds new elements to the end of the Series. When using Append, the
// Series is modified in place.
func (s *Series) Append(values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	if s.appendScalar(values) {
		return
	}
	news := New(values, s.t, s.Name)
	s.elements = s.elements.appendStore(news.elements)
}

func (s *Series) appendScalar(value interface{}) bool {
	if _, ok := value.(Series); ok {
		return false
	}
	if value != nil {
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice:
			return false
		}
	}
	switch s.t {
	case String, Int, Float, Bool, Time:
		appendAs(&s.elements, s.t, value)
	default:
		return false
	}
	return true
}

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s Series) Concat(x Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := x.Err; err != nil {
		s.Err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}
	y := s.Copy()
	y.Append(x)
	return y
}

// Subset returns a subset of the series based on the given Indexes.
func (s Series) Subset(indexes Indexes) Series {
	if err := s.Err; err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	return Series{
		Name:     s.Name,
		t:        s.t,
		elements: s.elements.gatherStore(idx),
	}
}

// Set sets the values on the indexes of a Series and returns the reference
// for itself. The original Series is modified.
func (s Series) Set(indexes Indexes, newvalues Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := newvalues.Err; err != nil {
		s.Err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	if len(idx) != newvalues.Len() {
		s.Err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}
	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.Err = fmt.Errorf("set error: index out of range")
			return s
		}
		if newvalues.IsNA(k) {
			setAt(&s.elements, s.t, i, "NaN")
		} else {
			setAt(&s.elements, s.t, i, newvalues.Val(k))
		}
	}
	return s
}

// HasNaN checks whether the Series contain NaN elements.
func (s Series) HasNaN() bool {
	if s.elements == nil {
		return false
	}
	return s.elements.storeHasNA()
}

// IsNaN returns an array that identifies which of the elements are NaN.
func (s Series) IsNaN() []bool {
	if s.elements == nil {
		return nil
	}
	return s.elements.storeIsNA()
}

func (s Series) FillNaN(value Series) Series {
	for p, isNaN := range s.IsNaN() {
		if isNaN {
			s.Set(p, value)
		}
	}
	return s
}

// fillCopyRange copies values inside a cloned buffer to fill missing slots.
// forward fills each missing slot with the closest preceding valid value;
// backward fills with the closest following valid value. limit <= 0 fills
// unbounded runs, otherwise only the first limit slots of every run.
func fillCopyRange[T any](col *column[T], forward bool, limit int) {
	if !col.storeHasNA() {
		return
	}
	col.ensureValidity()
	n := len(col.data)
	if forward {
		last := -1
		streak := 0
		for i := 0; i < n; i++ {
			if col.validity.get(i) {
				last = i
				streak = 0
				continue
			}
			streak++
			if last >= 0 && (limit <= 0 || streak <= limit) {
				col.data[i] = col.data[last]
				col.validity.set(i)
			}
		}
	} else {
		next := -1
		streak := 0
		for i := n - 1; i >= 0; i-- {
			if col.validity.get(i) {
				next = i
				streak = 0
				continue
			}
			streak++
			if next >= 0 && (limit <= 0 || streak <= limit) {
				col.data[i] = col.data[next]
				col.validity.set(i)
			}
		}
	}
}

func (s Series) fillNaCopy(forward bool, limit int) Series {
	result := s.Copy()
	switch col := result.elements.(type) {
	case intElements:
		fillCopyRange(&col, forward, limit)
		result.elements = col
	case floatElements:
		fillCopyRange(&col, forward, limit)
		result.elements = col
	case stringElements:
		fillCopyRange(&col, forward, limit)
		result.elements = col
	case boolElements:
		fillCopyRange(&col, forward, limit)
		result.elements = col
	case timeElements:
		fillCopyRange(&col, forward, limit)
		result.elements = col
	}
	return result
}

// FillNaNForward fills NaN values with the most recent non-NaN value that
// precedes them (forward fill / ffill).  Leading NaNs that have no predecessor
// are left as NaN.
func (s Series) FillNaNForward() Series { return s.fillNaCopy(true, 0) }

// FillNaNBackward fills NaN values with the nearest non-NaN value that
// follows them (backward fill / bfill).  Trailing NaNs that have no successor
// are left as NaN.
func (s Series) FillNaNBackward() Series { return s.fillNaCopy(false, 0) }

// FillNaNForwardLimit fills NaN values with the most recent non-NaN value,
// but only for up to `limit` consecutive NaN positions.
// limit <= 0 means no limit (equivalent to FillNaNForward).
func (s Series) FillNaNForwardLimit(limit int) Series { return s.fillNaCopy(true, limit) }

// FillNaNBackwardLimit fills NaN values with the nearest following non-NaN value,
// but only for up to `limit` consecutive NaN positions.
// limit <= 0 means no limit (equivalent to FillNaNBackward).
func (s Series) FillNaNBackwardLimit(limit int) Series { return s.fillNaCopy(false, limit) }

// Compare compares the values of a Series with other elements. To do so, the
// elements which are to be compared are first transformed to a Series of the
// same type as the caller. The evaluation runs on the typed comparison
// kernels producing a selection mask (kernel.go); the Bool Series result
// keeps the historical public signature.
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err; err != nil {
		return s
	}
	mask, err := s.CompareMask(comparator, comparando)
	if err != nil {
		ret := s.Empty()
		ret.Err = err
		return ret
	}
	return Series{t: Bool, elements: mask.m.toBoolColumn()}
}

// Copy will return a copy of the Series.
func (s Series) Copy() Series {
	if s.elements == nil {
		return Series{Name: s.Name, t: s.t, Err: s.Err}
	}
	return Series{
		Name:     s.Name,
		t:        s.t,
		elements: s.elements.cloneStore(),
		Err:      s.Err,
	}
}

// Records returns the elements of a Series as a []string
func (s Series) Records() []string {
	ret := make([]string, s.Len())
	for i := range ret {
		ret[i] = s.Record(i)
	}
	return ret
}

// Float returns the elements of a Series as a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := range ret {
		ret[i] = s.FloatAt(i)
	}
	return ret
}

// Int returns the elements of a Series as a []int or an error if the
// transformation is not possible.
func (s Series) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := range ret {
		val, err := s.IntAt(i)
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

func (s Series) Int64() []int64 {
	ret := make([]int64, s.Len())
	for i := range ret {
		val, err := s.Int64At(i)
		if err != nil {
			ret[i] = 0
		} else {
			ret[i] = val
		}
	}
	return ret
}

// Bool returns the elements of a Series as a []bool or an error if the
// transformation is not possible.
func (s Series) Bool() ([]bool, error) {
	ret := make([]bool, s.Len())
	for i := range ret {
		val, err := s.BoolAt(i)
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Type returns the type of a given series
func (s Series) Type() Type {
	return s.t
}

// Len returns the length of a given Series
func (s Series) Len() int {
	if s.elements == nil {
		return 0
	}
	return s.elements.storeLen()
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.Records())
}

// Str prints some extra information about a given series
func (s Series) Str() string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// parseIndexes will parse the given indexes for a given series of length `l`. No
// out of bounds checks is performed.
func parseIndexes(l int, indexes Indexes) ([]int, error) {
	var idx []int
	switch idxs := indexes.(type) {
	case []int:
		idx = idxs
	case int:
		idx = []int{idxs}
	case []bool:
		bools := idxs
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := idxs
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// ParseIndexes normalizes any supported Series row indexer into integer row
// positions. It performs the same validation as Subset and Set.
func ParseIndexes(l int, indexes Indexes) ([]int, error) {
	return parseIndexes(l, indexes)
}

// Order returns the indexes for sorting a Series. NaN elements are pushed to the
// end by order of appearance.
func (s Series) Order(reverse bool) []int {
	n := s.Len()
	valid := make([]int, 0, n)
	var nasIdx []int
	for i := 0; i < n; i++ {
		if s.IsNA(i) {
			nasIdx = append(nasIdx, i)
		} else {
			valid = append(valid, i)
		}
	}
	less := orderLessFunc(s)
	if reverse {
		sort.SliceStable(valid, func(a, b int) bool {
			return less(valid[b], valid[a])
		})
	} else {
		sort.SliceStable(valid, func(a, b int) bool {
			return less(valid[a], valid[b])
		})
	}
	return append(valid, nasIdx...)
}

// orderLessFunc returns a typed row-comparison closure for sorting; it is
// only called on valid rows.
func orderLessFunc(s Series) func(i, j int) bool {
	switch elems := s.elements.(type) {
	case intElements:
		return func(i, j int) bool { return elems.data[i] < elems.data[j] }
	case floatElements:
		return func(i, j int) bool { return elems.data[i] < elems.data[j] }
	case stringElements:
		return func(i, j int) bool { return elems.data[i] < elems.data[j] }
	case boolElements:
		return func(i, j int) bool { return !elems.data[i] && elems.data[j] }
	case timeElements:
		return func(i, j int) bool { return elems.data[i].Before(elems.data[j]) }
	}
	return func(i, j int) bool { return false }
}

// StdDev calculates the standard deviation of a series
func (s Series) StdDev() float64 {
	return stat.StdDev(s.Float(), nil)
}

// Mean calculates the average value of a series. Float columns walk the
// buffer directly without materializing a []float64 copy; NaN values
// propagate, matching the v1 behavior through Float().
func (s Series) Mean() float64 {
	if s.Len() == 0 {
		return math.NaN()
	}
	switch elems := s.elements.(type) {
	case floatElements:
		if elems.validity == nil {
			return stat.Mean(elems.data, nil)
		}
		return math.NaN()
	case intElements:
		if elems.validity != nil {
			return math.NaN()
		}
		var sum float64
		for _, v := range elems.data {
			sum += float64(v)
		}
		return sum / float64(len(elems.data))
	default:
		var sum float64
		for i := 0; i < s.Len(); i++ {
			sum += s.FloatAt(i)
		}
		return sum / float64(s.Len())
	}
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
func (s Series) Median() float64 {
	if s.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ordered := s.Subset(s.Order(false))
	n := ordered.Len()
	// When length is odd, we just take length(list)/2
	// value as the median.
	if n%2 != 0 {
		return ordered.FloatAt(n / 2)
	}
	// When length is even, we take middle two elements of
	// list and the median is an average of the two of them.
	return (ordered.FloatAt(n/2-1) + ordered.FloatAt(n/2)) * 0.5
}

// Max return the biggest element in the series. A missing first element keeps
// the result NaN, matching v1 behavior.
func (s Series) Max() float64 {
	if s.Len() == 0 || s.Type() == String {
		return math.NaN()
	}
	if s.IsNA(0) {
		return math.NaN()
	}
	max := s.FloatAt(0)
	for i := 1; i < s.Len(); i++ {
		if !s.IsNA(i) {
			if v := s.FloatAt(i); v > max {
				max = v
			}
		}
	}
	return max
}

// MaxStr return the biggest element in a series of type String
func (s Series) MaxStr() string {
	if s.Len() == 0 || s.Type() != String {
		return ""
	}
	if s.IsNA(0) {
		return "NaN"
	}
	max := s.Record(0)
	for i := 1; i < s.Len(); i++ {
		if !s.IsNA(i) {
			if v := s.Record(i); v > max {
				max = v
			}
		}
	}
	return max
}

// Min return the lowest element in the series. A missing first element keeps
// the result NaN, matching v1 behavior.
func (s Series) Min() float64 {
	if s.Len() == 0 || s.Type() == String {
		return math.NaN()
	}
	if s.IsNA(0) {
		return math.NaN()
	}
	min := s.FloatAt(0)
	for i := 1; i < s.Len(); i++ {
		if !s.IsNA(i) {
			if v := s.FloatAt(i); v < min {
				min = v
			}
		}
	}
	return min
}

// MinStr return the lowest element in a series of type String
func (s Series) MinStr() string {
	if s.Len() == 0 || s.Type() != String {
		return ""
	}
	if s.IsNA(0) {
		return "NaN"
	}
	min := s.Record(0)
	for i := 1; i < s.Len(); i++ {
		if !s.IsNA(i) {
			if v := s.Record(i); v < min {
				min = v
			}
		}
	}
	return min
}

// Quantile returns the sample of x such that x is greater than or
// equal to the fraction p of samples.
// Note: gonum/stat panics when called with strings
func (s Series) Quantile(p float64) float64 {
	if s.Type() == String || s.Len() == 0 {
		return math.NaN()
	}

	ordered := s.Subset(s.Order(false)).Float()

	return stat.Quantile(p, stat.Empirical, ordered, nil)
}

// Sum calculates the sum value of a series
func (s Series) Sum() float64 {
	if s.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	var sum float64
	switch elems := s.elements.(type) {
	case floatElements:
		for i, v := range elems.data {
			if elems.isValid(i) {
				sum += v
			}
		}
	case intElements:
		for i, v := range elems.data {
			if elems.isValid(i) {
				sum += float64(v)
			}
		}
	case timeElements:
		for i, v := range elems.data {
			if elems.isValid(i) {
				sum += float64(v.Unix())
			}
		}
	default:
		for i := 0; i < s.Len(); i++ {
			if !s.IsNA(i) {
				sum += s.FloatAt(i)
			}
		}
	}
	return sum
}

// Slice slices Series from j to k-1 index.
func (s Series) Slice(j, k int) Series {
	if s.Err != nil {
		return s
	}

	if j > k || j < 0 || k >= s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	idxs := make([]int, k-j)
	for i := 0; j+i < k; i++ {
		idxs[i] = j + i
	}

	return s.Subset(idxs)
}

// ValueCounts returns a map from each unique string representation of an element
// to its occurrence count.  NaN values are counted under the key "NaN".
func (s Series) ValueCounts() map[string]int {
	counts := make(map[string]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		counts[s.Record(i)]++
	}
	return counts
}

// Unique returns a new Series containing only the first occurrence of each
// distinct value (preserving original order).
func (s Series) Unique() Series {
	seen := make(map[string]struct{}, s.Len())
	var idxs []int
	for i := 0; i < s.Len(); i++ {
		key := s.Record(i)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			idxs = append(idxs, i)
		}
	}
	return s.Subset(idxs)
}

// NUnique returns the number of distinct non-NaN values in the Series.
func (s Series) NUnique() int {
	seen := make(map[string]struct{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) {
			continue
		}
		seen[s.Record(i)] = struct{}{}
	}
	return len(seen)
}

// CumSum returns a new Float Series containing the cumulative sum.
// NaN values are propagated (a NaN in input produces NaN from that point).
func (s Series) CumSum() Series {
	result := newFloatSeries(s.Name, s.Len())
	var cum float64
	hasNaN := false
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) || hasNaN {
			hasNaN = true
			result.Append(math.NaN())
		} else {
			cum += s.FloatAt(i)
			result.Append(cum)
		}
	}
	return result
}

// CumProd returns a new Float Series containing the cumulative product.
func (s Series) CumProd() Series {
	result := newFloatSeries(s.Name, s.Len())
	cum := 1.0
	hasNaN := false
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) || hasNaN {
			hasNaN = true
			result.Append(math.NaN())
		} else {
			cum *= s.FloatAt(i)
			result.Append(cum)
		}
	}
	return result
}

// CumMax returns a new Float Series containing the cumulative maximum.
func (s Series) CumMax() Series {
	result := newFloatSeries(s.Name, s.Len())
	curMax := math.NaN()
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) {
			result.Append(math.NaN())
		} else {
			v := s.FloatAt(i)
			if math.IsNaN(curMax) || v > curMax {
				curMax = v
			}
			result.Append(curMax)
		}
	}
	return result
}

// CumMin returns a new Float Series containing the cumulative minimum.
func (s Series) CumMin() Series {
	result := newFloatSeries(s.Name, s.Len())
	curMin := math.NaN()
	for i := 0; i < s.Len(); i++ {
		if s.IsNA(i) {
			result.Append(math.NaN())
		} else {
			v := s.FloatAt(i)
			if math.IsNaN(curMin) || v < curMin {
				curMin = v
			}
			result.Append(curMin)
		}
	}
	return result
}

// Diff returns a new Float Series of first-order differences (s[i] - s[i-periods]).
// periods can be negative for backward differences. Leading/trailing positions
// without a valid predecessor/successor are NaN.
func (s Series) Diff(periods int) Series {
	n := s.Len()
	result := newFloatSeries(s.Name, n)
	for i := 0; i < n; i++ {
		j := i - periods
		if j < 0 || j >= n {
			result.Append(math.NaN())
			continue
		}
		if s.IsNA(i) || s.IsNA(j) {
			result.Append(math.NaN())
		} else {
			result.Append(s.FloatAt(i) - s.FloatAt(j))
		}
	}
	return result
}

// PctChange returns element-wise percentage change: (s[i] - s[i-periods]) / abs(s[i-periods]).
// Equivalent to pandas Series.pct_change().
func (s Series) PctChange(periods int) Series {
	n := s.Len()
	result := newFloatSeries(s.Name, n)
	for i := 0; i < n; i++ {
		j := i - periods
		if j < 0 || j >= n {
			result.Append(math.NaN())
			continue
		}
		if s.IsNA(i) || s.IsNA(j) {
			result.Append(math.NaN())
			continue
		}
		prevVal := s.FloatAt(j)
		if prevVal == 0 {
			result.Append(math.NaN())
		} else {
			result.Append((s.FloatAt(i) - prevVal) / math.Abs(prevVal))
		}
	}
	return result
}

// Corr returns the Pearson correlation coefficient between s and other.
// Both Series must have the same length. NaN pairs are skipped.
func (s Series) Corr(other Series) float64 {
	if s.Len() != other.Len() {
		return math.NaN()
	}
	n := s.Len()
	var sumX, sumY, sumXY, sumX2, sumY2 float64
	count := 0
	for i := 0; i < n; i++ {
		if s.IsNA(i) || other.IsNA(i) {
			continue
		}
		x, y := s.FloatAt(i), other.FloatAt(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
		sumY2 += y * y
		count++
	}
	if count < 2 {
		return math.NaN()
	}
	fc := float64(count)
	num := fc*sumXY - sumX*sumY
	den := math.Sqrt((fc*sumX2 - sumX*sumX) * (fc*sumY2 - sumY*sumY))
	if den == 0 {
		return math.NaN()
	}
	return num / den
}

// Cov returns the sample covariance between s and other (ddof=1).
// NaN pairs are skipped.
func (s Series) Cov(other Series) float64 {
	if s.Len() != other.Len() {
		return math.NaN()
	}
	n := s.Len()
	var sumX, sumY, sumXY float64
	count := 0
	for i := 0; i < n; i++ {
		if s.IsNA(i) || other.IsNA(i) {
			continue
		}
		x, y := s.FloatAt(i), other.FloatAt(i)
		sumX += x
		sumY += y
		sumXY += x * y
		count++
	}
	if count < 2 {
		return math.NaN()
	}
	fc := float64(count)
	return (sumXY - sumX*sumY/fc) / (fc - 1)
}

// StringsDirect constructs a String Series by copying the provided slice into
// a column buffer. The "NaN" sentinel string becomes a missing value.
func StringsDirect(values []string) Series {
	data := make([]string, len(values))
	copy(data, values)
	col := stringElements{data: data}
	for i, v := range values {
		if v == "NaN" {
			col.setNA(i)
		}
	}
	return Series{t: String, elements: col}
}

// FloatsDirect constructs a Float Series by copying the provided slice into
// a column buffer. NaN payloads become missing values.
func FloatsDirect(values []float64) Series {
	data := make([]float64, len(values))
	copy(data, values)
	col := floatElements{data: data}
	for i, v := range values {
		if v != v {
			col.setNA(i)
		}
	}
	return Series{t: Float, elements: col}
}

// IntsDirect constructs an Int Series by copying the provided slice into a
// column buffer.
func IntsDirect(values []int) Series {
	data := make([]int64, len(values))
	for i, v := range values {
		data[i] = int64(v)
	}
	return Series{t: Int, elements: intElements{data: data}}
}

// BoolsDirect constructs a Bool Series by copying the provided slice into a
// column buffer.
func BoolsDirect(values []bool) Series {
	data := make([]bool, len(values))
	copy(data, values)
	return Series{t: Bool, elements: boolElements{data: data}}
}

// TimesDirect constructs a Time Series by copying the provided slice into a
// column buffer.
func TimesDirect(values []time.Time) Series {
	data := make([]time.Time, len(values))
	copy(data, values)
	return Series{t: Time, elements: timeElements{data: data}}
}

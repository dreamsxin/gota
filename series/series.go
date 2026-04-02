package series

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"math"

	"gonum.org/v1/gonum/stat"
)

// Series is a data structure designed for operating on arrays of elements that
// should comply with a certain type structure. They are flexible enough that can
// be transformed to other Series types and account for missing or non valid
// elements. Most of the power of Series resides on the ability to compare and
// subset Series of different types.
type Series struct {
	Name     string   // The name of the series
	elements Elements // The values of the elements
	t        Type     // The type of the series

	// deprecated: use Error() instead
	Err error
}

// Elements is the interface that represents the array of elements contained on
// a Series.
type Elements interface {
	Elem(int) Element
	Len() int
}

// Element is the interface that defines the types of methods to be present for
// elements of a Series
type Element interface {
	// Setter method
	Set(interface{})

	// Comparation methods
	Eq(Element) bool
	Neq(Element) bool
	Less(Element) bool
	LessEq(Element) bool
	Greater(Element) bool
	GreaterEq(Element) bool

	// Accessor/conversion methods
	Copy() Element     // FIXME: Returning interface is a recipe for pain
	Val() ElementValue // FIXME: Returning interface is a recipe for pain
	String() string
	Int() (int, error)
	Int64() (int64, error)
	Float() float64
	Bool() (bool, error)
	Time() (time.Time, error)

	// Information methods
	IsNA() bool
	Type() Type
}

// intElements is the concrete implementation of Elements for Int elements.
type intElements []intElement

func (e intElements) Len() int           { return len(e) }
func (e intElements) Elem(i int) Element { return &e[i] }

// stringElements is the concrete implementation of Elements for String elements.
type stringElements []stringElement

func (e stringElements) Len() int           { return len(e) }
func (e stringElements) Elem(i int) Element { return &e[i] }

// floatElements is the concrete implementation of Elements for Float elements.
type floatElements []floatElement

func (e floatElements) Len() int           { return len(e) }
func (e floatElements) Elem(i int) Element { return &e[i] }

// boolElements is the concrete implementation of Elements for Bool elements.
type boolElements []boolElement

func (e boolElements) Len() int           { return len(e) }
func (e boolElements) Elem(i int) Element { return &e[i] }

// timeElement is the concrete implementation of Elements for time elements.
type timeElements []timeElement

func (e timeElements) Len() int           { return len(e) }
func (e timeElements) Elem(i int) Element { return &e[i] }

// ElementValue represents the value that can be used for marshaling or
// unmarshaling Elements.
type ElementValue interface{}

type MapFunction func(Element) Element

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

// compFunc defines a user-defined comparator function. Used internally for type assertions
type compFunc = func(el Element) bool

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

// New is the generic Series constructor
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// Pre-allocate elements
	preAlloc := func(n int) {
		switch t {
		case String:
			ret.elements = make(stringElements, n)
		case Int:
			ret.elements = make(intElements, n)
		case Float:
			ret.elements = make(floatElements, n)
		case Bool:
			ret.elements = make(boolElements, n)
		case Time:
			ret.elements = make(timeElements, n)
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}

	if values == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []float64:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []int:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []bool:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case Series:
		l := v.Len()
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v.elements.Elem(i))
		}
	default:
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			v := reflect.ValueOf(values)
			l := v.Len()
			preAlloc(v.Len())
			for i := 0; i < l; i++ {
				val := v.Index(i).Interface()
				ret.elements.Elem(i).Set(val)
			}
		default:
			preAlloc(1)
			v := reflect.ValueOf(values)
			val := v.Interface()
			ret.elements.Elem(0).Set(val)
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
	return New([]int{}, s.t, s.Name)
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
	for i := s.elements.Len(); i < num; i++ {
		switch s.t {
		case String:
			s.elements = append(s.elements.(stringElements), news.elements.(stringElements)...)
		case Int:
			s.elements = append(s.elements.(intElements), news.elements.(intElements)...)
		case Float:
			s.elements = append(s.elements.(floatElements), news.elements.(floatElements)...)
		case Bool:
			s.elements = append(s.elements.(boolElements), news.elements.(boolElements)...)
		case Time:
			s.elements = append(s.elements.(timeElements), news.elements.(timeElements)...)
		}
	}
}

// Append adds new elements to the end of the Series. When using Append, the
// Series is modified in place.
func (s *Series) Append(values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	news := New(values, s.t, s.Name)
	switch s.t {
	case String:
		s.elements = append(s.elements.(stringElements), news.elements.(stringElements)...)
	case Int:
		s.elements = append(s.elements.(intElements), news.elements.(intElements)...)
	case Float:
		s.elements = append(s.elements.(floatElements), news.elements.(floatElements)...)
	case Bool:
		s.elements = append(s.elements.(boolElements), news.elements.(boolElements)...)
	case Time:
		s.elements = append(s.elements.(timeElements), news.elements.(timeElements)...)
	}
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
	ret := Series{
		Name: s.Name,
		t:    s.t,
	}
	switch s.t {
	case String:
		elements := make(stringElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(stringElements)[i]
		}
		ret.elements = elements
	case Int:
		elements := make(intElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(intElements)[i]
		}
		ret.elements = elements
	case Float:
		elements := make(floatElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(floatElements)[i]
		}
		ret.elements = elements
	case Bool:
		elements := make(boolElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(boolElements)[i]
		}
		ret.elements = elements
	case Time:
		elements := make(timeElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(timeElements)[i]
		}
		ret.elements = elements
	default:
		panic("unknown series type")
	}
	return ret
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
		s.elements.Elem(i).Set(newvalues.elements.Elem(k))
	}
	return s
}

// HasNaN checks whether the Series contain NaN elements.
func (s Series) HasNaN() bool {
	for i := 0; i < s.Len(); i++ {
		if s.elements.Elem(i).IsNA() {
			return true
		}
	}
	return false
}

// IsNaN returns an array that identifies which of the elements are NaN.
func (s Series) IsNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.elements.Elem(i).IsNA()
	}
	return ret
}

func (s Series) FillNaN(value Series) Series {
	for p, isNaN := range s.IsNaN() {
		if isNaN {
			s.Set(p, value)
		}
	}
	return s
}

// FillNaNForward fills NaN values with the most recent non-NaN value that
// precedes them (forward fill / ffill).  Leading NaNs that have no predecessor
// are left as NaN.
func (s Series) FillNaNForward() Series {
	result := s.Copy()
	var lastVal interface{}
	for i := 0; i < result.Len(); i++ {
		elem := result.Elem(i)
		if elem.IsNA() {
			if lastVal != nil {
				elem.Set(lastVal)
			}
		} else {
			lastVal = elem.Val()
		}
	}
	return result
}

// FillNaNBackward fills NaN values with the nearest non-NaN value that
// follows them (backward fill / bfill).  Trailing NaNs that have no successor
// are left as NaN.
func (s Series) FillNaNBackward() Series {
	result := s.Copy()
	var nextVal interface{}
	for i := result.Len() - 1; i >= 0; i-- {
		elem := result.Elem(i)
		if elem.IsNA() {
			if nextVal != nil {
				elem.Set(nextVal)
			}
		} else {
			nextVal = elem.Val()
		}
	}
	return result
}

// Compare compares the values of a Series with other elements. To do so, the
// elements with are to be compared are first transformed to a Series of the same
// type as the caller.
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err; err != nil {
		return s
	}
	compareElements := func(a, b Element, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	bools := make([]bool, s.Len())

	// CompFunc comparator comparison
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}

		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			bools[i] = f(e)
		}

		return Bools(bools)
	}

	comp := New(comparando, s.t, "")
	// In comparator comparison
	if comparator == In { // Inside
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			b := false
			for j := 0; j < comp.Len(); j++ {
				m := comp.elements.Elem(j)
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.Err = err
					return s
				}
				if c {
					b = true
					break
				}
			}
			bools[i] = b
		}
		return Bools(bools)
	}

	if comparator == Out { // Outside
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			b := true
			for j := 0; j < comp.Len(); j++ {
				m := comp.elements.Elem(j)
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.Err = err
					return s
				}
				if c {
					b = false
					break
				}
			}
			bools[i] = b
		}
		return Bools(bools)
	}

	// Single element comparison
	if comp.Len() == 1 {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			c, err := compareElements(e, comp.elements.Elem(0), comparator)
			if err != nil {
				s = s.Empty()
				s.Err = err
				return s
			}
			bools[i] = c
		}
		return Bools(bools)
	}

	// Multiple element comparison
	if s.Len() != comp.Len() {
		s := s.Empty()
		s.Err = fmt.Errorf("can't compare: length mismatch")
		return s
	}
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		c, err := compareElements(e, comp.elements.Elem(i), comparator)
		if err != nil {
			s = s.Empty()
			s.Err = err
			return s
		}
		bools[i] = c
	}
	return Bools(bools)
}

// Copy will return a copy of the Series.
func (s Series) Copy() Series {
	name := s.Name
	t := s.t
	err := s.Err
	var elements Elements
	switch s.t {
	case String:
		elements = make(stringElements, s.Len())
		copy(elements.(stringElements), s.elements.(stringElements))
	case Float:
		elements = make(floatElements, s.Len())
		copy(elements.(floatElements), s.elements.(floatElements))
	case Bool:
		elements = make(boolElements, s.Len())
		copy(elements.(boolElements), s.elements.(boolElements))
	case Int:
		elements = make(intElements, s.Len())
		copy(elements.(intElements), s.elements.(intElements))
	case Time:
		elements = make(timeElements, s.Len())
		copy(elements.(timeElements), s.elements.(timeElements))
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
		Err:      err,
	}
	return ret
}

// Records returns the elements of a Series as a []string
func (s Series) Records() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.String()
	}
	return ret
}

// Float returns the elements of a Series as a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s Series) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.Float()
	}
	return ret
}

// Int returns the elements of a Series as a []int or an error if the
// transformation is not possible.
func (s Series) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

func (s Series) Int64() []int64 {
	ret := make([]int64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int64()
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
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Bool()
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
	return s.elements.Len()
}

// String implements the Stringer interface for Series
func (s Series) String() string {
	return fmt.Sprint(s.elements)
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

// Val returns the value of a series for the given index. Will panic if the index
// is out of bounds.
func (s Series) Val(i int) interface{} {
	if s.elements == nil {
		return nil
	}
	return s.elements.Elem(i).Val()
}

// Elem returns the element of a series for the given index. Will panic if the
// index is out of bounds.
func (s Series) Elem(i int) Element {
	if s.elements == nil {
		return nil
	}
	return s.elements.Elem(i)
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

// Order returns the indexes for sorting a Series. NaN elements are pushed to the
// end by order of appearance.
func (s Series) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e})
		}
	}
	var srt sort.Interface
	srt = ie
	if reverse {
		srt = sort.Reverse(srt)
	}
	sort.Stable(srt)
	var ret []int
	for _, e := range ie {
		ret = append(ret, e.index)
	}
	return append(ret, nasIdx...)
}

type indexedElement struct {
	index   int
	element Element
}

type indexedElements []indexedElement

func (e indexedElements) Len() int           { return len(e) }
func (e indexedElements) Less(i, j int) bool { return e[i].element.Less(e[j].element) }
func (e indexedElements) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// StdDev calculates the standard deviation of a series
func (s Series) StdDev() float64 {
	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean calculates the average value of a series
func (s Series) Mean() float64 {
	stdDev := stat.Mean(s.Float(), nil)
	return stdDev
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
func (s Series) Median() float64 {
	if s.elements.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ix := s.Order(false)
	newElem := make([]Element, len(ix))

	for newpos, oldpos := range ix {
		newElem[newpos] = s.elements.Elem(oldpos)
	}

	// When length is odd, we just take length(list)/2
	// value as the median.
	if len(newElem)%2 != 0 {
		return newElem[len(newElem)/2].Float()
	}
	// When length is even, we take middle two elements of
	// list and the median is an average of the two of them.
	return (newElem[(len(newElem)/2)-1].Float() +
		newElem[len(newElem)/2].Float()) * 0.5
}

// Max return the biggest element in the series
func (s Series) Max() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.Float()
}

// MaxStr return the biggest element in a series of type String
func (s Series) MaxStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.String()
}

// Min return the lowest element in the series
func (s Series) Min() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.Float()
}

// MinStr return the lowest element in a series of type String
func (s Series) MinStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.String()
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

// Map applies a function matching MapFunction signature, which itself
// allowing for a fairly flexible MAP implementation, intended for mapping
// the function over each element in Series and returning a new Series object.
// Function must be compatible with the underlying type of data in the Series.
// In other words it is expected that when working with a Float Series, that
// the function passed in via argument `f` will not expect another type, but
// instead expects to handle Element(s) of type Float.
func (s Series) Map(f MapFunction) Series {
	mappedValues := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i))
		mappedValues[i] = value
	}
	return New(mappedValues, s.Type(), s.Name)
}

// Sum calculates the sum value of a series
func (s Series) Sum() float64 {
	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	var sum float64
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if !e.IsNA() {
			sum += e.Float()
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
		counts[s.Elem(i).String()]++
	}
	return counts
}

// Unique returns a new Series containing only the first occurrence of each
// distinct value (preserving original order).
func (s Series) Unique() Series {
	seen := make(map[string]struct{}, s.Len())
	var idxs []int
	for i := 0; i < s.Len(); i++ {
		key := s.Elem(i).String()
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
		elem := s.Elem(i)
		if elem.IsNA() {
			continue
		}
		seen[elem.String()] = struct{}{}
	}
	return len(seen)
}

// CumSum returns a new Float Series containing the cumulative sum.
// NaN values are propagated (a NaN in input produces NaN from that point).
func (s Series) CumSum() Series {
	result := New([]float64{}, Float, s.Name)
	var cum float64
	hasNaN := false
	for i := 0; i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.IsNA() || hasNaN {
			hasNaN = true
			result.Append(math.NaN())
		} else {
			cum += elem.Float()
			result.Append(cum)
		}
	}
	return result
}

// CumProd returns a new Float Series containing the cumulative product.
func (s Series) CumProd() Series {
	result := New([]float64{}, Float, s.Name)
	cum := 1.0
	hasNaN := false
	for i := 0; i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.IsNA() || hasNaN {
			hasNaN = true
			result.Append(math.NaN())
		} else {
			cum *= elem.Float()
			result.Append(cum)
		}
	}
	return result
}

// CumMax returns a new Float Series containing the cumulative maximum.
func (s Series) CumMax() Series {
	result := New([]float64{}, Float, s.Name)
	curMax := math.NaN()
	for i := 0; i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.IsNA() {
			result.Append(math.NaN())
		} else {
			v := elem.Float()
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
	result := New([]float64{}, Float, s.Name)
	curMin := math.NaN()
	for i := 0; i < s.Len(); i++ {
		elem := s.Elem(i)
		if elem.IsNA() {
			result.Append(math.NaN())
		} else {
			v := elem.Float()
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
	result := New([]float64{}, Float, s.Name)
	n := s.Len()
	for i := 0; i < n; i++ {
		j := i - periods
		if j < 0 || j >= n {
			result.Append(math.NaN())
			continue
		}
		cur := s.Elem(i)
		prev := s.Elem(j)
		if cur.IsNA() || prev.IsNA() {
			result.Append(math.NaN())
		} else {
			result.Append(cur.Float() - prev.Float())
		}
	}
	return result
}

// PctChange returns element-wise percentage change: (s[i] - s[i-periods]) / abs(s[i-periods]).
// Equivalent to pandas Series.pct_change().
func (s Series) PctChange(periods int) Series {
	result := New([]float64{}, Float, s.Name)
	n := s.Len()
	for i := 0; i < n; i++ {
		j := i - periods
		if j < 0 || j >= n {
			result.Append(math.NaN())
			continue
		}
		cur := s.Elem(i)
		prev := s.Elem(j)
		if cur.IsNA() || prev.IsNA() {
			result.Append(math.NaN())
			continue
		}
		prevVal := prev.Float()
		if prevVal == 0 {
			result.Append(math.NaN())
		} else {
			result.Append((cur.Float() - prevVal) / math.Abs(prevVal))
		}
	}
	return result
}

// FillNaNForwardLimit fills NaN values with the most recent non-NaN value,
// but only for up to `limit` consecutive NaN positions.
// limit <= 0 means no limit (equivalent to FillNaNForward).
func (s Series) FillNaNForwardLimit(limit int) Series {
	result := s.Copy()
	var lastVal interface{}
	streak := 0
	for i := 0; i < result.Len(); i++ {
		elem := result.Elem(i)
		if elem.IsNA() {
			streak++
			if lastVal != nil && (limit <= 0 || streak <= limit) {
				elem.Set(lastVal)
			}
		} else {
			lastVal = elem.Val()
			streak = 0
		}
	}
	return result
}

// FillNaNBackwardLimit fills NaN values with the nearest following non-NaN value,
// but only for up to `limit` consecutive NaN positions.
// limit <= 0 means no limit (equivalent to FillNaNBackward).
func (s Series) FillNaNBackwardLimit(limit int) Series {
	result := s.Copy()
	var nextVal interface{}
	streak := 0
	for i := result.Len() - 1; i >= 0; i-- {
		elem := result.Elem(i)
		if elem.IsNA() {
			streak++
			if nextVal != nil && (limit <= 0 || streak <= limit) {
				elem.Set(nextVal)
			}
		} else {
			nextVal = elem.Val()
			streak = 0
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
		a := s.Elem(i)
		b := other.Elem(i)
		if a.IsNA() || b.IsNA() {
			continue
		}
		x, y := a.Float(), b.Float()
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
		a := s.Elem(i)
		b := other.Elem(i)
		if a.IsNA() || b.IsNA() {
			continue
		}
		x, y := a.Float(), b.Float()
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

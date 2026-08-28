package series

import "strconv"

// DType is the logical column type of the v2 kernel (RFC §4). Physical
// storage types map one-to-one onto the Series types; logical types such as
// Dictionary add metadata over a physical layout. Decimal and ordered Enum
// will land on this same interface without storage changes.
type DType interface {
	// Physical returns the underlying physical storage type.
	Physical() PhysicalType
	// Metadata carries logical-type attributes (e.g. dictionary
	// cardinality, enum ordering). Physical types return nil.
	Metadata() map[string]string
}

// PhysicalType enumerates the kernel's physical storage layouts.
type PhysicalType string

// Supported physical types.
const (
	PhysInt64      PhysicalType = "int64"
	PhysFloat64    PhysicalType = "float64"
	PhysUtf8       PhysicalType = "utf8"
	PhysBool       PhysicalType = "bool"
	PhysTime       PhysicalType = "time"
	PhysDictionary PhysicalType = "dictionary"
)

// physicalDType is the singleton DType for a physical layout.
type physicalDType struct {
	phys PhysicalType
}

func (p physicalDType) Physical() PhysicalType      { return p.phys }
func (p physicalDType) Metadata() map[string]string { return nil }
func (p physicalDType) String() string              { return string(p.phys) }

// Physical DType singletons.
var (
	DTInt64   DType = physicalDType{PhysInt64}
	DTFloat64 DType = physicalDType{PhysFloat64}
	DTUtf8    DType = physicalDType{PhysUtf8}
	DTBool    DType = physicalDType{PhysBool}
	DTTime    DType = physicalDType{PhysTime}
)

// dictionaryDType is the first logical DType: strings stored as codes into a
// shared category dictionary (it absorbs the standalone Categorical).
type dictionaryDType struct {
	categories []string
	ordered    bool
}

// NewDictionaryDType returns a Dictionary logical DType over the given
// categories. ordered marks an ordinal (enum-like) category set.
func NewDictionaryDType(categories []string, ordered bool) DType {
	cats := make([]string, len(categories))
	copy(cats, categories)
	return dictionaryDType{categories: cats, ordered: ordered}
}

func (d dictionaryDType) Physical() PhysicalType { return PhysDictionary }

func (d dictionaryDType) Metadata() map[string]string {
	return map[string]string{
		"cardinality": strconv.Itoa(len(d.categories)),
		"ordered":     strconv.FormatBool(d.ordered),
	}
}

func (d dictionaryDType) String() string {
	return "dictionary(" + strconv.Itoa(len(d.categories)) + ")"
}

// DictionaryCategories extracts the category list from a Dictionary DType.
// The boolean result is false for any other DType.
func DictionaryCategories(dt DType) ([]string, bool) {
	d, ok := dt.(dictionaryDType)
	if !ok {
		return nil, false
	}
	out := make([]string, len(d.categories))
	copy(out, d.categories)
	return out, true
}

// DTypeOf maps a Series type to its physical DType.
func DTypeOf(t Type) DType {
	switch t {
	case Int:
		return DTInt64
	case Float:
		return DTFloat64
	case String:
		return DTUtf8
	case Bool:
		return DTBool
	case Time:
		return DTTime
	}
	return nil
}

// DType returns the logical DType of the Series: the Dictionary DType for
// dictionary-encoded columns, otherwise the physical DType of the type.
func (s Series) DType() DType {
	if elems, ok := s.elements.(dictionaryElements); ok {
		return elems.dType()
	}
	return DTypeOf(s.t)
}

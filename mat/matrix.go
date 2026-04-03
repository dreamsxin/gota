package mat

import (
	"fmt"
	"math"

	"github.com/dreamsxin/gota/series"
)

type Type string

const (
	TypeMul Type = "mul"
	TypeDiv Type = "div"
	TypeSub Type = "Sub"
	TypeAdd Type = "Add"
)

type Mode string

const (
	ModeNone Mode = "none"
	ModeZero Mode = "zero"
	ModeOne  Mode = "one"
)

// Cal performs element-wise calculation between two series with padding mode.
// It validates inputs and handles length mismatches according to mod parameter.
func Cal(a series.Series, b series.Series, op Type, mod Mode) series.Series {
	// Validate inputs
	if a.Err != nil {
		ret := series.New([]interface{}{}, series.Float, "")
		ret.Err = fmt.Errorf("mat.Cal: input a has error: %v", a.Err)
		return ret
	}
	if b.Err != nil {
		ret := series.New([]interface{}{}, series.Float, "")
		ret.Err = fmt.Errorf("mat.Cal: input b has error: %v", b.Err)
		return ret
	}

	la := a.Len()
	lb := b.Len()

	// Create copies to avoid modifying originals
	acopy := a.Copy()
	bcopy := b.Copy()

	// Handle length mismatch according to mode
	switch mod {
	case ModeZero:
		if la > lb {
			bcopy.Fill(la, 0)
		} else if la < lb {
			acopy.Fill(lb, 0)
		}
	case ModeOne:
		if la > lb {
			bcopy.Fill(la, 1)
		} else if la < lb {
			acopy.Fill(lb, 1)
		}
		// ModeNone: keep original lengths, operations will handle mismatch
	}

	// Perform operation
	var c series.Series
	switch op {
	case TypeMul:
		c = Mul(acopy, bcopy)
	case TypeDiv:
		c = Div(acopy, bcopy)
	case TypeSub:
		c = Sub(acopy, bcopy)
	case TypeAdd:
		c = Add(acopy, bcopy)
	default:
		ret := series.New([]interface{}{}, series.Float, "")
		ret.Err = fmt.Errorf("mat.Cal: unknown operation type: %v", op)
		return ret
	}

	return c
}

// Mul performs element-wise multiplication of two series.
// Returns Float series if either input is Float, otherwise Int series.
// Handles length mismatch by using minimum length.
func Mul(a series.Series, b series.Series) series.Series {
	la := a.Len()
	lb := b.Len()
	minLen := la
	if lb < minLen {
		minLen = lb
	}

	// Determine result type based on input types
	resultIsFloat := a.Type() == series.Float || b.Type() == series.Float

	if resultIsFloat {
		c := series.Floats([]float64{})
		av := a.Float()
		bv := b.Float()
		for i := 0; i < minLen; i++ {
			c.Append(av[i] * bv[i])
		}
		return c
	}

	c := series.Ints([]int64{})
	av := a.Int64()
	bv := b.Int64()
	for i := 0; i < minLen; i++ {
		c.Append(av[i] * bv[i])
	}
	return c
}

// Div performs element-wise division of two series.
// Always returns Float series. Division by zero results in NaN (not 0).
// Handles length mismatch by using minimum length.
func Div(a series.Series, b series.Series) series.Series {
	la := a.Len()
	lb := b.Len()
	minLen := la
	if lb < minLen {
		minLen = lb
	}

	c := series.Floats([]float64{})
	av := a.Float()
	bv := b.Float()
	for i := 0; i < minLen; i++ {
		if bv[i] == 0 {
			c.Append(math.NaN())
		} else {
			c.Append(av[i] / bv[i])
		}
	}
	return c
}

// Sub performs element-wise subtraction of two series (a - b).
// Returns Float series if a is Float, otherwise Int series.
// Handles length mismatch by using minimum length.
func Sub(a series.Series, b series.Series) series.Series {
	la := a.Len()
	lb := b.Len()
	minLen := la
	if lb < minLen {
		minLen = lb
	}

	if a.Type() == series.Float {
		c := series.Floats([]float64{})
		av := a.Float()
		bv := b.Float()
		for i := 0; i < minLen; i++ {
			c.Append(av[i] - bv[i])
		}
		return c
	}

	c := series.Ints([]int64{})
	av := a.Int64()
	bv := b.Int64()
	for i := 0; i < minLen; i++ {
		c.Append(av[i] - bv[i])
	}
	return c
}

// Add performs element-wise addition of two series.
// Returns Float series if a is Float, otherwise Int series.
// Handles length mismatch by appending remaining elements from longer series.
func Add(a series.Series, b series.Series) series.Series {
	la := a.Len()
	lb := b.Len()

	if a.Type() == series.Float {
		c := series.Floats([]float64{})
		av := a.Float()
		bv := b.Float()

		if la >= lb {
			for i := 0; i < la; i++ {
				if i >= lb {
					c.Append(av[i])
					continue
				}
				c.Append(av[i] + bv[i])
			}
		} else {
			for i := 0; i < lb; i++ {
				if i >= la {
					c.Append(bv[i])
					continue
				}
				c.Append(av[i] + bv[i])
			}
		}
		return c
	}

	c := series.Ints([]int64{})
	av := a.Int64()
	bv := b.Int64()

	if la >= lb {
		for i := 0; i < la; i++ {
			if i >= lb {
				c.Append(av[i])
				continue
			}
			c.Append(av[i] + bv[i])
		}
	} else {
		for i := 0; i < lb; i++ {
			if i >= la {
				c.Append(bv[i])
				continue
			}
			c.Append(av[i] + bv[i])
		}
	}
	return c
}

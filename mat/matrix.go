package mat

import (
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

func Cal(a series.Series, b series.Series, op Type, mod Mode) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()
	switch mod {
	case ModeZero:
		if la > lb {
			b.Fill(la, 0)
		} else if la < lb {
			a.Fill(lb, 0)
		}
	case ModeOne:
		if la > lb {
			b.Fill(la, 1)
		} else if la < lb {
			a.Fill(lb, 1)
		}
	}

	switch op {
	case TypeMul:
		c = Mul(a, b)
	case TypeDiv:
		c = Div(a, b)
	case TypeSub:
		c = Sub(a, b)
	case TypeAdd:
		c = Add(a, b)
	}
	return c
}

func Mul(a series.Series, b series.Series) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()
	if a.Type() != series.Int || b.Type() != series.Int {

		c = series.Floats([]float64{})
		if la >= lb {
			av := a.Float()
			for i, v := range b.Float() {
				c.Append(av[i] * v)
			}
		} else {
			bv := a.Float()
			for i, v := range a.Float() {
				c.Append(v * bv[i])
			}
		}
	} else {
		c = series.Ints([]int64{})
		if la >= lb {
			av := a.Int64()
			for i, v := range b.Int64() {
				c.Append(av[i] * v)
			}
		} else {
			bv := a.Int64()
			for i, v := range a.Int64() {
				c.Append(v * bv[i])
			}
		}
	}
	return c
}

func Div(a series.Series, b series.Series) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()

	c = series.Floats([]float64{})
	if la >= lb {
		av := a.Float()
		for i, v := range b.Float() {
			if v != 0 {
				c.Append(av[i] / v)
			} else {
				c.Append(0)
			}
		}
	} else {
		bv := a.Float()
		for i, v := range a.Float() {
			if v != 0 {
				c.Append(v * bv[i])
			} else {
				c.Append(0)
			}
		}
	}
	return c
}

func Sub(a series.Series, b series.Series) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()
	switch a.Type() {
	case series.Float:
		c = series.Floats([]float64{})
		if la >= lb {
			av := a.Float()
			for i, v := range b.Float() {
				c.Append(av[i] - v)
			}
		} else {
			bv := a.Float()
			for i, v := range a.Float() {
				c.Append(v - bv[i])
			}
		}
	default:
		c = series.Ints([]int64{})
		if la >= lb {
			av := a.Int64()
			for i, v := range b.Int64() {
				c.Append(av[i] - v)
			}
		} else {
			bv := a.Int64()
			for i, v := range a.Int64() {
				c.Append(v - bv[i])
			}
		}
	}
	return c
}

func Add(a series.Series, b series.Series) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()
	switch a.Type() {
	case series.Float:
		c = series.Floats([]float64{})
		if la >= lb {
			av := a.Float()
			for i, v := range b.Float() {
				c.Append(av[i] + v)
			}
		} else {
			bv := b.Float()
			for i, v := range a.Float() {
				c.Append(v + bv[i])
			}
		}
	default:
		c = series.Ints([]int64{})
		if la >= lb {
			av := a.Int64()
			for i, v := range b.Int64() {
				c.Append(av[i] + v)
			}
		} else {
			bv := b.Int64()
			for i, v := range a.Int64() {
				c.Append(v + bv[i])
			}
		}
	}
	return c
}

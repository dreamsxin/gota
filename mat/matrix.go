package mat

import (
	"github.com/dreamsxin/gota/series"
)

func Dot(a series.Series, b series.Series) series.Series {
	var c series.Series
	la := a.Len()
	lb := b.Len()
	switch a.Type() {
	case series.Float:
		c = series.Floats([]float64{})
		if la >= lb {
			av := a.Float()
			for i, v := range b.Float() {
				c.Append(v * av[i])
			}
		} else {
			bv := a.Float()
			for i, v := range a.Float() {
				c.Append(v * bv[i])
			}
		}
	default:
		c = series.Ints([]int64{})
		if la >= lb {
			av := a.Int64()
			for i, v := range b.Int64() {
				c.Append(v * av[i])
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

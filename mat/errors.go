package mat

// Error represents matrix handling errors. These errors can be recovered by Maybe wrappers.
type Error struct{ string }

func (err Error) Error() string { return err.string }

var (
	ErrNegativeDimension   = Error{"mat: negative dimension"}
	ErrIndexOutOfRange     = Error{"mat: index out of range"}
	ErrReuseNonEmpty       = Error{"mat: reuse of non-empty matrix"}
	ErrRowAccess           = Error{"mat: row index out of range"}
	ErrColAccess           = Error{"mat: column index out of range"}
	ErrVectorAccess        = Error{"mat: vector index out of range"}
	ErrZeroLength          = Error{"mat: zero length in matrix dimension"}
	ErrRowLength           = Error{"mat: row length mismatch"}
	ErrColLength           = Error{"mat: col length mismatch"}
	ErrSquare              = Error{"mat: expect square matrix"}
	ErrNormOrder           = Error{"mat: invalid norm order for matrix"}
	ErrSingular            = Error{"mat: matrix is singular"}
	ErrShape               = Error{"mat: dimension mismatch"}
	ErrIllegalStride       = Error{"mat: illegal stride"}
	ErrPivot               = Error{"mat: malformed pivot list"}
	ErrTriangle            = Error{"mat: triangular storage mismatch"}
	ErrTriangleSet         = Error{"mat: triangular set out of bounds"}
	ErrBandwidth           = Error{"mat: bandwidth out of range"}
	ErrBandSet             = Error{"mat: band set out of bounds"}
	ErrDiagSet             = Error{"mat: diagonal set out of bounds"}
	ErrSliceLengthMismatch = Error{"mat: input slice length mismatch"}
	ErrNotPSD              = Error{"mat: input not positive symmetric definite"}
	ErrFailedEigen         = Error{"mat: eigendecomposition not successful"}
)

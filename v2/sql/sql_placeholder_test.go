package sql

import "testing"

// Internal unit tests for the placeholder builder (migrated from the former
// dataframe-package v1.5 suite).

func TestWriteSQL_DollarPlaceholder(t *testing.T) {
	// SQLite doesn't support $1 style, but we can verify the generated SQL
	// by checking that the placeholder builder produces the right strings.
	ph := buildPlaceholder(SQLPlaceholderDollar, 1)
	if ph != "$1" {
		t.Errorf("dollar placeholder: got %q want $1", ph)
	}
	ph3 := buildPlaceholder(SQLPlaceholderDollar, 3)
	if ph3 != "$3" {
		t.Errorf("dollar placeholder 3: got %q want $3", ph3)
	}
}

func TestWriteSQL_AtPlaceholder(t *testing.T) {
	ph := buildPlaceholder(SQLPlaceholderAt, 2)
	if ph != "@p2" {
		t.Errorf("at placeholder: got %q want @p2", ph)
	}
}

func TestWriteSQL_QuestionPlaceholder(t *testing.T) {
	ph := buildPlaceholder(SQLPlaceholderQuestion, 99)
	if ph != "?" {
		t.Errorf("question placeholder: got %q want ?", ph)
	}
}

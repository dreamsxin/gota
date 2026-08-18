// Package doccheck contains documentation consistency checks that run as
// part of the normal test suite: README anchor links must resolve to real
// headings using GitHub's anchor algorithm.
package doccheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot walks up from the package directory to the directory holding
// go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above package directory")
		}
		dir = parent
	}
}

// githubAnchor converts a heading to its GitHub anchor: lowercase, drop
// anything that is not a letter, digit, space, or hyphen, then replace
// spaces with hyphens.
func githubAnchor(heading string) string {
	heading = strings.ToLower(heading)
	var sb strings.Builder
	for _, r := range heading {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == ' ', r == '-':
			sb.WriteRune(r)
		}
	}
	return strings.ReplaceAll(sb.String(), " ", "-")
}

var (
	headingRe  = regexp.MustCompile(`^#{1,6}\s+(.*)$`)
	setextRe   = regexp.MustCompile(`^(=|-){2,}$`)
	anchorLink = regexp.MustCompile(`\]\(#([^)]+)\)`)
)

func collectHeadings(t *testing.T, lines []string) map[string]bool {
	t.Helper()
	anchors := make(map[string]bool)
	for i, line := range lines {
		if m := headingRe.FindStringSubmatch(line); m != nil {
			anchors[githubAnchor(m[1])] = true
			continue
		}
		// Setext heading: text directly underlined by = or -.
		if i+1 < len(lines) && line != "" && setextRe.MatchString(lines[i+1]) {
			if headingRe.MatchString(line) {
				continue
			}
			anchors[githubAnchor(line)] = true
		}
	}
	return anchors
}

func checkAnchorLinks(t *testing.T, file string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), file))
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	content := string(raw)
	// Normalize CRLF checkouts so the checks hold on any platform.
	content = strings.ReplaceAll(content, "\r\n", "\n")
	anchors := collectHeadings(t, strings.Split(content, "\n"))

	seen := make(map[string]bool)
	for _, m := range anchorLink.FindAllStringSubmatch(content, -1) {
		target := m[1]
		if seen[target] {
			continue
		}
		seen[target] = true
		if !anchors[target] {
			t.Errorf("%s links to #%s but no heading generates that anchor", file, target)
		}
	}
}

func TestREADMEAnchorLinks(t *testing.T) {
	checkAnchorLinks(t, "README.md")
}

func TestROADMAPAnchorLinks(t *testing.T) {
	checkAnchorLinks(t, "ROADMAP.md")
}

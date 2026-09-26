package contract

import "testing"

func TestParseSemverCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.1", -1},
		{"1.2.0", "1.1.9", 1},
		{"2.0.0", "1.9.9", 1},
		{"1.0.0-rc.1", "1.0.0", -1},
		{"1.0.0-alpha", "1.0.0-beta", -1},
		{"1.0.0", "1.0.0", 0},
		{"1.0", "1.0.0", 0},
		{"bogus", "0.0.1", -1},
	}
	for _, c := range cases {
		if got := parseSemver(c.a).compare(parseSemver(c.b)); got != c.want {
			t.Errorf("compare(%s,%s)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

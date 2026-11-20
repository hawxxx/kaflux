package api

import (
	"strings"
	"testing"
)

func TestParseExcluder(t *testing.T) {
	if e, err := parseExcluder("", ""); e != nil || err != nil {
		t.Fatalf("nothing given should give nil, got %v %v", e, err)
	}
	terms := func(n int) string {
		return `{"include":[` + strings.TrimSuffix(strings.Repeat(`{"op":"equals","value":"a"},`, n), ",") + `]}`
	}
	for name, c := range map[string][2]string{
		"bad regex":      {"(", ""},
		"long regex":     {strings.Repeat("a", 201), ""},
		"bad json":       {"", "{"},
		"a list":         {"", `[{"op":"starts","value":"a"}]`},
		"empty value":    {"", `{"include":[{"op":"starts","value":""}]}`},
		"unknown op":     {"", `{"include":[{"op":"like","value":"a"}]}`},
		"regex term":     {"", `{"except":[{"op":"regex","value":"a"}]}`},
		"long value":     {"", `{"include":[{"op":"starts","value":"` + strings.Repeat("a", 201) + `"}]}`},
		"too many terms": {"", terms(21)},
	} {
		if _, err := parseExcluder(c[0], c[1]); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := parseExcluder("", terms(20)); err != nil {
		t.Fatalf("20 terms rejected: %v", err)
	}
}

func TestExcluderMatch(t *testing.T) {
	var nilE *nameExcluder
	if nilE.Match("x") {
		t.Fatal("a nil excluder excluded a topic")
	}
	cases := []struct {
		name   string
		regex  string
		filter string
		topic  string
		expect bool
	}{
		{"regex only", `^tmp\.`, "", "tmp.a", true},
		{"regex only miss", `^tmp\.`, "", "prod.a", false},
		{"include any", "", `{"include":[{"op":"starts","value":"tmp"},{"op":"starts","value":"test"}]}`, "test-1", true},
		{"include miss", "", `{"include":[{"op":"starts","value":"tmp"},{"op":"starts","value":"test"}]}`, "prod", false},
		{"except keeps the topic", "", `{"include":[{"op":"starts","value":"tmp"},{"op":"starts","value":"test"}],"except":[{"op":"contains","value":"keep"}]}`, "tmp-keep", false},
		{"except keeps a test topic", "", `{"include":[{"op":"starts","value":"tmp"},{"op":"starts","value":"test"}],"except":[{"op":"contains","value":"keep"}]}`, "test-keep-1", false},
		{"except leaves the rest excluded", "", `{"include":[{"op":"starts","value":"tmp"},{"op":"starts","value":"test"}],"except":[{"op":"contains","value":"keep"}]}`, "tmp-x", true},
		{"except alone excludes nothing", "", `{"except":[{"op":"starts","value":"prod"}]}`, "dev-x", false},
		{"ends and equals", "", `{"include":[{"op":"ends","value":"-dlq"},{"op":"equals","value":"scratch"}]}`, "orders-dlq", true},
		{"equals is exact", "", `{"include":[{"op":"equals","value":"scratch"}]}`, "scratch2", false},
		{"regex joins the includes", `^x`, `{"include":[{"op":"equals","value":"y"}]}`, "y", true},
		{"regex hit is still vetoed", `^x`, `{"except":[{"op":"contains","value":"keep"}]}`, "x-keep", false},
		{"neither", `^x`, `{"include":[{"op":"equals","value":"y"}]}`, "z", false},
	}
	for _, c := range cases {
		e, err := parseExcluder(c.regex, c.filter)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := e.Match(c.topic); got != c.expect {
			t.Fatalf("%s: Match(%q) = %v, want %v", c.name, c.topic, got, c.expect)
		}
	}
}

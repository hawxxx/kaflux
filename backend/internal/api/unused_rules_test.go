package api

import "testing"

func TestParseExcluder(t *testing.T) {
	if e, err := parseExcluder("", ""); e != nil || err != nil {
		t.Fatalf("nothing given should give nil, got %v %v", e, err)
	}
	for name, c := range map[string][2]string{
		"bad regex":      {"(", ""},
		"long regex":     {string(make([]byte, 201)), ""},
		"bad json":       {"", "{"},
		"empty value":    {"", `[{"op":"starts","value":""}]`},
		"unknown op":     {"", `[{"op":"like","value":"a"}]`},
		"missing join":   {"", `[{"op":"starts","value":"a"},{"op":"ends","value":"b"}]`},
		"bad rule regex": {"", `[{"op":"regex","value":"("}]`},
		"too many":       {"", "[" + repeat(`{"op":"equals","value":"a","join":"or"},`, 20) + `{"op":"equals","value":"a","join":"or"}]`},
	} {
		if _, err := parseExcluder(c[0], c[1]); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func TestExcluderMatch(t *testing.T) {
	var nilE *nameExcluder
	if nilE.Match("x") {
		t.Fatal("a nil excluder excluded a topic")
	}
	cases := []struct {
		name   string
		regex  string
		rules  string
		topic  string
		expect bool
	}{
		{"regex only", `^tmp\.`, "", "tmp.a", true},
		{"regex only miss", `^tmp\.`, "", "prod.a", false},
		{"starts or starts", "", `[{"op":"starts","value":"tmp"},{"op":"starts","value":"test","join":"or"}]`, "test-1", true},
		{"or miss", "", `[{"op":"starts","value":"tmp"},{"op":"starts","value":"test","join":"or"}]`, "prod", false},
		{"and not keeps the topic", "", `[{"op":"starts","value":"tmp"},{"op":"contains","value":"keep","not":true,"join":"and"}]`, "tmp-keep", false},
		{"and not excludes the rest", "", `[{"op":"starts","value":"tmp"},{"op":"contains","value":"keep","not":true,"join":"and"}]`, "tmp-x", true},
		{"not alone excludes everything else", "", `[{"op":"starts","value":"prod","not":true}]`, "dev-x", true},
		{"not alone keeps matches", "", `[{"op":"starts","value":"prod","not":true}]`, "prod-x", false},
		{"and binds tighter than or", "", `[{"op":"equals","value":"a"},{"op":"starts","value":"b","join":"or"},{"op":"contains","value":"c","join":"and"}]`, "bc", true},
		{"and binds tighter than or miss", "", `[{"op":"equals","value":"a"},{"op":"starts","value":"b","join":"or"},{"op":"contains","value":"c","join":"and"}]`, "b", false},
		{"ends and regex rule", "", `[{"op":"ends","value":"-dlq"},{"op":"regex","value":"^[a-z]+-","join":"and"}]`, "orders-dlq", true},
		{"regex or rules", `^x`, `[{"op":"equals","value":"y"}]`, "xa", true},
		{"regex or rules second", `^x`, `[{"op":"equals","value":"y"}]`, "y", true},
		{"neither", `^x`, `[{"op":"equals","value":"y"}]`, "z", false},
	}
	for _, c := range cases {
		e, err := parseExcluder(c.regex, c.rules)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := e.Match(c.topic); got != c.expect {
			t.Fatalf("%s: Match(%q) = %v, want %v", c.name, c.topic, got, c.expect)
		}
	}
}

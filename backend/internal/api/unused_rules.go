package api

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const (
	maxExcludeRules      = 20
	maxExcludeValueBytes = 200
)

// excludeRule is one condition on a topic name. Join says how it connects to the rule
// before it (and, or); Not negates the condition itself.
type excludeRule struct {
	Op    string `json:"op"`
	Value string `json:"value"`
	Not   bool   `json:"not"`
	Join  string `json:"join"`
}

type compiledRule struct {
	rule excludeRule
	re   *regexp.Regexp
}

// nameExcluder decides which topics the cleanup report leaves out. A topic is excluded
// when the optional regular expression matches or the rule expression is true. In the
// rule expression AND binds tighter than OR, as in most query languages.
type nameExcluder struct {
	re    *regexp.Regexp
	rules []compiledRule
}

// parseExcluder builds an excluder from the optional regex and the optional JSON rule list.
// It returns nil when neither is given.
func parseExcluder(pattern, rulesJSON string) (*nameExcluder, error) {
	var out nameExcluder
	if pattern != "" {
		if len(pattern) > maxExcludeValueBytes {
			return nil, errors.New("exclude must be at most 200 characters")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, errors.New("exclude is not a valid regular expression")
		}
		out.re = re
	}
	if rulesJSON != "" {
		var rules []excludeRule
		if len(rulesJSON) > 16*1024 || json.Unmarshal([]byte(rulesJSON), &rules) != nil {
			return nil, errors.New("rules must be a JSON list of at most 20 rules")
		}
		if len(rules) > maxExcludeRules {
			return nil, errors.New("rules must be a JSON list of at most 20 rules")
		}
		for i, r := range rules {
			c := compiledRule{rule: r}
			if r.Value == "" || len(r.Value) > maxExcludeValueBytes {
				return nil, errors.New("each rule needs a value of at most 200 characters")
			}
			switch r.Op {
			case "starts", "ends", "contains", "equals":
			case "regex":
				re, err := regexp.Compile(r.Value)
				if err != nil {
					return nil, errors.New("a rule is not a valid regular expression")
				}
				c.re = re
			default:
				return nil, errors.New("rule op must be starts, ends, contains, equals or regex")
			}
			if i > 0 && r.Join != "and" && r.Join != "or" {
				return nil, errors.New("rule join must be and or or")
			}
			out.rules = append(out.rules, c)
		}
	}
	if out.re == nil && len(out.rules) == 0 {
		return nil, nil
	}
	return &out, nil
}

func (c compiledRule) matches(name string) bool {
	var hit bool
	switch c.rule.Op {
	case "starts":
		hit = strings.HasPrefix(name, c.rule.Value)
	case "ends":
		hit = strings.HasSuffix(name, c.rule.Value)
	case "contains":
		hit = strings.Contains(name, c.rule.Value)
	case "equals":
		hit = name == c.rule.Value
	case "regex":
		hit = c.re.MatchString(name)
	}
	return hit != c.rule.Not
}

// Match reports whether the topic is excluded. A nil excluder excludes nothing.
func (e *nameExcluder) Match(name string) bool {
	if e == nil {
		return false
	}
	if e.re != nil && e.re.MatchString(name) {
		return true
	}
	// OR of AND-chains: a chain breaks at each "or" join.
	chain := true
	for i, r := range e.rules {
		if i > 0 && r.rule.Join == "or" {
			if chain {
				return true
			}
			chain = true
		}
		chain = chain && r.matches(name)
	}
	return len(e.rules) > 0 && chain
}

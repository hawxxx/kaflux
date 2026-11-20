package api

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const (
	maxNameTerms         = 20
	maxExcludeValueBytes = 200
)

// nameTerm is one condition on a topic name.
type nameTerm struct {
	Op    string `json:"op"`
	Value string `json:"value"`
}

// nameFilter is the JSON form of the keyword filter: any include term leaves a topic
// out, unless an except term matches, which always keeps it.
type nameFilter struct {
	Include []nameTerm `json:"include"`
	Except  []nameTerm `json:"except"`
}

// nameExcluder decides which topics the cleanup report leaves out. A topic is left out
// when the optional regular expression or any include term matches, and no except term does.
type nameExcluder struct {
	re      *regexp.Regexp
	include []nameTerm
	except  []nameTerm
}

func validTerms(terms []nameTerm) error {
	if len(terms) > maxNameTerms {
		return errors.New("at most 20 include and 20 except terms")
	}
	for _, t := range terms {
		if t.Value == "" || len(t.Value) > maxExcludeValueBytes {
			return errors.New("each term needs a value of at most 200 characters")
		}
		switch t.Op {
		case "starts", "ends", "contains", "equals":
		default:
			return errors.New("term op must be starts, ends, contains or equals")
		}
	}
	return nil
}

// parseExcluder builds an excluder from the optional regex and the optional JSON filter.
// It returns nil when neither is given.
func parseExcluder(pattern, filterJSON string) (*nameExcluder, error) {
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
	if filterJSON != "" {
		var f nameFilter
		if len(filterJSON) > 16*1024 || json.Unmarshal([]byte(filterJSON), &f) != nil {
			return nil, errors.New("rules must be a JSON object with include and except term lists")
		}
		if err := validTerms(f.Include); err != nil {
			return nil, err
		}
		if err := validTerms(f.Except); err != nil {
			return nil, err
		}
		out.include, out.except = f.Include, f.Except
	}
	if out.re == nil && len(out.include) == 0 && len(out.except) == 0 {
		return nil, nil
	}
	return &out, nil
}

func (t nameTerm) matches(name string) bool {
	switch t.Op {
	case "starts":
		return strings.HasPrefix(name, t.Value)
	case "ends":
		return strings.HasSuffix(name, t.Value)
	case "contains":
		return strings.Contains(name, t.Value)
	case "equals":
		return name == t.Value
	}
	return false
}

func anyTerm(terms []nameTerm, name string) bool {
	for _, t := range terms {
		if t.matches(name) {
			return true
		}
	}
	return false
}

// Match reports whether the topic is left out. A nil excluder leaves out nothing, and a
// filter with only except terms leaves out nothing either, since nothing selects topics.
func (e *nameExcluder) Match(name string) bool {
	if e == nil {
		return false
	}
	if !(e.re != nil && e.re.MatchString(name)) && !anyTerm(e.include, name) {
		return false
	}
	return !anyTerm(e.except, name)
}

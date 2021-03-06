package schema

import (
	"sort"
	"strings"
)

type Schema struct {
	Fields    []string
	StartLine int
	EndLine   int
}

func (s *Schema) Key() string {
	sort.Strings(s.Fields)
	return strings.Join(s.Fields, ",")
}


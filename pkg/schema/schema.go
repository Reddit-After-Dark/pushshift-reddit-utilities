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


type Schemas struct {
	schemas []Schema
}

func (s *Schemas) AddSchema(newSchema Schema) bool {
	for _, schema := range s.schemas {
		if schema.Key() == newSchema.Key() {
			return false
		}
	}

	s.schemas = append(s.schemas, newSchema)
	return true
}

func (s *Schemas) GetSchema(newSchema Schema) bool {

}


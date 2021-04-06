package schema

import (
	"sort"
	"strings"
)

type Schema struct {
	Fields    []string `json:"fields"`
}

func (s *Schema) Key() string {
	sort.Strings(s.Fields)
	return strings.Join(s.Fields, ",")
}

type Schemas struct {
	Schemas []Schema `json:"schemas"`
}

func (s *Schemas) AddSchema(newSchema Schema) {
	for _, schema := range s.Schemas {
		if schema.Key() == newSchema.Key() {
			return
		}
	}

	s.Schemas = append(s.Schemas, newSchema)
	return
}

func (s *Schemas) GetSchemaID(newSchema Schema) int {
	s.AddSchema(newSchema)

	for index, schema := range s.Schemas {
		if schema.Key() == newSchema.Key() {
			return index
		}
	}

	return 0
}

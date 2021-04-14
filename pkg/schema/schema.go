package schema

import (
	"sort"
	"strings"
)

type Schema struct {
	Fields []string `json:"fields"`
	Count  int      `json:"count"`
}

func (s *Schema) Key() string {
	sort.Strings(s.Fields)
	return strings.Join(s.Fields, ",")
}

type Schemas struct {
	Schemas map[int]Schema `json:"schemas"`

	index int
}

func NewSchemas() Schemas {
	return Schemas{
		Schemas: make(map[int]Schema),
		index:   1,
	}
}

func (s *Schemas) AddSchema(newSchema Schema) {
	for _, schema := range s.Schemas {
		if schema.Key() == newSchema.Key() {
			return
		}
	}

	s.Schemas[s.index] = newSchema
	s.index++
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

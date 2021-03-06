package detector

import (
	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/schema"
	"github.com/valyala/fastjson"
	"sync"
)

type SchemaCounter struct {
	Schemas map[int]map[string]int
	parser  fastjson.Parser

	sync.Mutex
}

func NewSchemaCounter() *SchemaCounter {
	return &SchemaCounter{
		Schemas: make(map[int]map[string]int),
		parser:  fastjson.Parser{},
	}
}

func (s *SchemaCounter) Process(line []byte, metadata conveyor.LineMetadata) (out []byte, err error) {
	s.Lock()
	defer s.Unlock()

	v, err := s.parser.ParseBytes(line)
	if err != nil {
		return nil, err
	}

	object, err := v.Object()
	if err != nil {
		return nil, err
	}

	var fields []string
	object.Visit(func(key []byte, v *fastjson.Value) {
		fields = append(fields, string(key))
	})

	tempSchema := schema.Schema{Fields: fields}
	key := tempSchema.Key()

	chunkId := metadata.Chunk.Id

	_, set := s.Schemas[chunkId]
	if !set {
		s.Schemas[chunkId] = map[string]int{key: 1}
		return nil, nil
	}

	_, set = s.Schemas[chunkId][key]
	if !set {
		s.Schemas[chunkId][key] = 1
	} else {
		s.Schemas[chunkId][key] ++
	}

	return nil, nil
}

func (s *SchemaCounter) Result() map[string]int {
	result := make(map[string]int)

	for _, schemas := range s.Schemas {
		for schemaName, count := range schemas {
			_, set := result[schemaName]
			if set {
				result[schemaName] += count
			} else {
				result[schemaName] = count
			}
		}

	}

	return result
}

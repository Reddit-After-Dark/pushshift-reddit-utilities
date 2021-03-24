package detector

import (
	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/schema"
	"github.com/valyala/fastjson"
	"sync"
)



type SchemaCounter struct {
	Result map[conveyor.Chunk]map[int]int

	Schemas schema.Schemas

	parser fastjson.Parser
	sync.Mutex
}

func NewSchemaCounter() *SchemaCounter {
	return &SchemaCounter{
		Result: make(map[conveyor.Chunk]map[int]int),
		parser: fastjson.Parser{},
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

	ChunkId := metadata.Chunk.Id

	_, set := s.Result[schema.ChunkId(ChunkId)]
	if !set {
		s.Result[schema.ChunkId(ChunkId)] = map[schema.SchemaId]int{key: 1}
		return nil, nil
	}

	_, set = s.Result[ChunkId][key]
	if !set {
		s.Result[ChunkId][key] = 1
	} else {
		s.Result[ChunkId][key] ++
	}

	return nil, nil
}


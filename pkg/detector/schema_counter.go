package detector

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/schema"
	"github.com/valyala/fastjson"
)

type SchemaCounter struct {
	Result  map[int]ChunkResult
	Schemas schema.Schemas

	parser fastjson.Parser
	sync.Mutex
}

type ChunkResult struct {
	Meta    ChunkMeta   `json:"chunk_metadata"`
	Schemas map[int]int `json:"schemas"`
}

type ChunkMeta struct {
	ChunkId  int    `json:"chunk_id"`
	FileName string `json:"file_name"`
	Size     int    `json:"size"`
	Offset   int64  `json:"offset"`
	Lines    int    `json:"lines"`
}

type SchemaCounterResult struct {
	ChunkResults map[int]ChunkResult `json:"chunk_results"`
	Schemas      schema.Schemas      `json:"schemas"`
}

func NewSchemaCounter() *SchemaCounter {
	return &SchemaCounter{
		Result: make(map[int]ChunkResult),
		parser: fastjson.Parser{},
		Schemas: schema.NewSchemas(),
	}
}

func (s *SchemaCounter) IncrementSchema(schema schema.Schema, c *conveyor.Chunk) {
	_, ok := s.Result[c.Id]
	if !ok {
		s.Result[c.Id] = ChunkResult{
			Schemas: make(map[int]int),
		}
	}

	id := s.Schemas.GetSchemaID(schema)

	if _, set := s.Result[c.Id].Schemas[id]; !set {
		s.Result[c.Id].Schemas[id] = 1
	} else {
		s.Result[c.Id].Schemas[id] ++
	}
}

func (s *SchemaCounter) Process(line []byte, metadata conveyor.LineMetadata) (out []byte, err error) {
	s.Lock()
	defer s.Unlock()
	v, err := s.parser.ParseBytes(line)
	if err != nil {
		return nil, fmt.Errorf("cannot parse line: %w", err)
	}

	object, err := v.Object()
	if err != nil {
		return nil, err
	}

	var fields []string
	object.Visit(func(key []byte, v *fastjson.Value) {
		fields = append(fields, string(key))
	})

	tmpSchema := schema.Schema{Fields: fields}

	s.IncrementSchema(tmpSchema, metadata.Chunk)

	return nil, nil
}

func (s *SchemaCounter) GetResult(finishedChunks []conveyor.ChunkResult) (*SchemaCounterResult, error) {
	result := SchemaCounterResult{
		ChunkResults: s.Result,
		Schemas:      s.Schemas,
	}

	for id := range result.ChunkResults {
		r, err := getChunkResultForId(id, finishedChunks)
		if err != nil {
			return nil, err
		}

		if !r.Ok() {
			return nil, fmt.Errorf("chunk failed: %w", r.Err)
		}

		chunk := r.Chunk

		result.ChunkResults[id] = ChunkResult{
			Meta:    ChunkMeta{
				ChunkId:  chunk.Id,
				FileName: chunk.In.GetName(),
				Size:     chunk.RealSize,
				Offset:   chunk.RealOffset,
				Lines:    chunk.LinesProcessed,
			},
			Schemas: result.ChunkResults[id].Schemas,
		}

		for schemaID, count := range result.ChunkResults[id].Schemas {
			schemaItem := result.Schemas.Schemas[schemaID]
			schemaItem.Count += count
			result.Schemas.Schemas[schemaID] = schemaItem
		}

	}

	return &result, nil
}

func getChunkResultForId(chunkId int, chunkResults []conveyor.ChunkResult) (*conveyor.ChunkResult, error) {
	for _, chunkResult := range chunkResults {
		if chunkResult.Chunk.Id == chunkId {
			return &chunkResult, nil
		}
	}

	return nil, errors.New("no chunk result with that id found")
}

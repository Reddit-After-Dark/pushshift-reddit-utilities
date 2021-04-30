package detector

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/schema"
	"github.com/valyala/fastjson"
)

type SchemaCounter struct {
	Result  map[int]ChunkResult
	Schemas schema.Schemas

	parser map[int]*fastjson.Parser
	sync.Mutex
}

type ChunkResult struct {
	Meta    ChunkMeta   `json:"chunk"`
	Schemas map[int]int `json:"schemas"`

	FirstRetrievedOn int `json:"-"`
}

type ChunkMeta struct {
	ChunkId          int    `json:"chunk_id"`
	FirstRetrievedOn int    `json:"retrieved_on"`
	FileName         string `json:"file_name"`
	Size             int    `json:"size"`
	Offset           int64  `json:"offset"`
	Lines            int    `json:"lines"`
}

type SchemaCounterResult struct {
	ChunkResults map[int]ChunkResult `json:"chunk_results"`
	Schemas      schema.Schemas      `json:"schemas"`
}

func (s *SchemaCounterResult) MarshalJSON() ([]byte, error) {
	chunkResults := make([]ChunkResult, 0, len(s.ChunkResults))

	for _, result := range s.ChunkResults {
		chunkResults = append(chunkResults, result)
	}

	sort.Slice(chunkResults, func(i, j int) bool {
		return chunkResults[i].Meta.ChunkId < chunkResults[j].Meta.ChunkId
	})

	return json.Marshal(struct {
		ChunkResults []ChunkResult `json:"chunk_results"`
		schema.Schemas
	}{
		ChunkResults: chunkResults,
		Schemas:      s.Schemas,
	})
}

func NewSchemaCounter() *SchemaCounter {
	return &SchemaCounter{
		Result:  make(map[int]ChunkResult),
		parser:  make(map[int]*fastjson.Parser),
		Schemas: schema.NewSchemas(),
	}
}

func (s *SchemaCounter) IncrementSchema(schema schema.Schema, c *conveyor.Chunk, retrievedOn int) {
	s.Lock()
	defer s.Unlock()

	_, ok := s.Result[c.Id]
	if !ok {
		s.Result[c.Id] = ChunkResult{
			FirstRetrievedOn: retrievedOn,
			Schemas:          make(map[int]int),
		}
	}

	id := s.Schemas.GetSchemaID(schema)

	if _, set := s.Result[c.Id].Schemas[id]; !set {
		s.Result[c.Id].Schemas[id] = 1
	} else {
		s.Result[c.Id].Schemas[id] ++
	}
}

func (s *SchemaCounter) getParser(meta *conveyor.LineMetadata) *fastjson.Parser {
	s.Lock()
	defer s.Unlock()

	if _, set := s.parser[meta.WorkerId]; !set {
		s.parser[meta.WorkerId] = &fastjson.Parser{}
	}

	return s.parser[meta.WorkerId]
}

func (s *SchemaCounter) Process(line []byte, metadata conveyor.LineMetadata) (out []byte, err error) {
	parser := s.getParser(&metadata)

	v, err := parser.ParseBytes(line)
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

	retrievedOn, err := object.Get("retrieved_on").Int()
	if err != nil {
		return nil, err
	}

	tmpSchema := schema.Schema{Fields: fields}

	s.IncrementSchema(tmpSchema, metadata.Chunk, retrievedOn)

	return nil, nil
}

func (s *SchemaCounter) GetResult(finishedChunks []conveyor.ChunkResult) (*SchemaCounterResult, error) {
	result := SchemaCounterResult{
		ChunkResults: s.Result,
		Schemas:      s.Schemas,
	}

	for id := range result.ChunkResults {
		chunkResult := result.ChunkResults[id]
		r, err := getChunkResultForId(id, finishedChunks)
		if err != nil {
			return nil, err
		}

		if !r.Ok() {
			return nil, fmt.Errorf("chunk failed: %w", r.Err)
		}

		result.ChunkResults[id] = ChunkResult{
			Meta: ChunkMeta{
				ChunkId:          r.Chunk.Id,
				FileName:         r.Chunk.In.GetHandleID(),
				Size:             r.RealSize,
				Offset:           r.RealOffset,
				Lines:            r.Lines,
				FirstRetrievedOn: chunkResult.FirstRetrievedOn,
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

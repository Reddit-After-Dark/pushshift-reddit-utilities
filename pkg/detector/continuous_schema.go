package detector

import (
	"sync"

	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/schema"
	"github.com/valyala/fastjson"
)

type ContinuousSchema struct {
	Schemas map[int][]schema.Schema
	parser  fastjson.Parser

	sync.Mutex
}

func NewContinuousSchema() *ContinuousSchema {
	return &ContinuousSchema{
		Schemas: make(map[int][]schema.Schema),
		parser:  fastjson.Parser{},
	}
}

func (c *ContinuousSchema) Process(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	c.Lock()
	defer c.Unlock()

	v, err := c.parser.ParseBytes(line)
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

	chunkId := metadata.Chunk.Id

	_, set := c.Schemas[chunkId]
	if !set {
		c.Schemas[chunkId] = []schema.Schema{
			{
				Fields:    fields,
				StartLine: 1,
			},
		}

		return nil, nil
	}

	currentSchema := schema.Schema{
		Fields:    fields,
		StartLine: metadata.Line,
	}

	lastSchema := c.Schemas[chunkId][len(c.Schemas[chunkId])-1]
	if lastSchema.Key() != currentSchema.Key() {
		c.Schemas[chunkId] = append(c.Schemas[chunkId], currentSchema)
	}

	return nil, err
}

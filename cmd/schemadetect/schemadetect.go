package main

import (
	"encoding/json"
	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/detector"
	"log"
	"os"
)

func main() {
	chunks, err := conveyor.GetChunksFromFile("RC_2019-12", 10*1024*1024, nil)
	if err != nil {
		log.Fatal(err)
	}

	sd := detector.NewSchemaCounter()

	queueResult := conveyor.NewQueue(chunks[:10], 10, sd).Work()

	result, err := sd.GetResult(queueResult.Results)
	if err != nil {
		log.Fatal(err)
	}

	resultFile, _ := os.Create("RC_2019-12_schemas.json")
	encoder := json.NewEncoder(resultFile)

	_ = encoder.Encode(result)
}

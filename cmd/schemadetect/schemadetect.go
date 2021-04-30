package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/detector"
)

func main() {
	input := os.Args[1]
	output := os.Args[2]

	chunks, err := conveyor.GetChunksFromFile(input, 100*1024*1024, nil)
	if err != nil {
		log.Fatal(err)
	}

	sd := detector.NewSchemaCounter()

	queueResult := conveyor.NewQueue(chunks, 6, sd).Work()

	result, err := sd.GetResult(queueResult.Results)
	if err != nil {
		log.Fatal(err)
	}

	resultFile, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}

	encoder := json.NewEncoder(resultFile)

	err = encoder.Encode(result)
	if err != nil {
		log.Fatal(err)
	}
}

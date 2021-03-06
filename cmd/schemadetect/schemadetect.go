package main

import (
	"fmt"
	"github.com/fgehrlicher/conveyor"
	"github.com/fgehrlicher/pushshift-reddit-utilities/pkg/detector"
	"log"
)

func main() {
	chunks, err := conveyor.GetChunksFromFile("RC_2019-12", 100*1024*1024, nil)
	if err != nil {
		log.Fatal(err)
	}

	sd := detector.NewSchemaCounter()

	result := conveyor.NewQueue(chunks, 10, sd).Work()

	fmt.Printf(
		"processed %d lines.\n%d chunks failed.\n",
		result.Lines,
		result.FailedChunks,
	)

	r := sd.Result()

	for k, v := range r {
		fmt.Printf("%v\n%v\n\n", k, v)
	}
}

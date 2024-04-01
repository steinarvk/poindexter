package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/steinarvk/recdex/lib/flatten"
)

func mainCore(reader io.Reader) error {
	scanner := bufio.NewScanner(reader)

	const (
		maxLineLen = 1024 * 1024
	)

	// Handle longer lines
	buf := make([]byte, maxLineLen)
	scanner.Buffer(buf, maxLineLen)

	flattener := flatten.Flattener{
		MaxSerializedLength:       maxLineLen,
		MaxExploredObjectElements: 100,
		MaxTotalFields:            1000,
		MaxCapturedValueLength:    100,
	}

	for scanner.Scan() {
		line := scanner.Text()

		record, err := flattener.FlattenJSON([]byte(line))
		if err != nil {
			return err
		}

		fmt.Println(record.Hash)

		for _, k := range record.Fields {
			values, hasValue := record.FieldValues[k]
			if hasValue {
				for _, value := range values {
					fmt.Printf("%s\tvalue\t%s\n", k, string(value))
				}
			} else {
				fmt.Printf("%s\tpresent\n", k)
			}
		}
		fmt.Println()
	}

	return nil
}

func main() {
	if err := mainCore(os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

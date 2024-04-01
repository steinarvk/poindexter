package flatten

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	canonicaljson "github.com/gibson042/canonicaljson-go"
	"github.com/google/uuid"
)

type PathElementKind int

const (
	ObjectField PathElementKind = 1
	ArrayIndex  PathElementKind = 2
)

type PathElement struct {
	Kind       PathElementKind
	FieldName  string
	ArrayIndex int
}

type Flattener struct {
	MaxSerializedLength       int
	MaxExploredObjectElements int
	MaxTotalFields            int
	MaxCapturedValueLength    int
}

type Record struct {
	RecordID      string
	Timestamp     time.Time
	Hash          string
	FieldValues   map[string][][]byte
	Fields        []string
	CanonicalJSON string
}

func visitJSON(elements []PathElement, value interface{}, visit func([]PathElement, interface{}) (bool, error)) error {
	explore, err := visit(elements, value)
	if err != nil {
		return err
	}
	if !explore {
		return nil
	}

	switch value := value.(type) {
	case map[string]interface{}:
		for k, v := range value {
			elementsCopy := make([]PathElement, len(elements)+1)
			copy(elementsCopy, elements)
			elementsCopy[len(elements)] = PathElement{
				Kind:      ObjectField,
				FieldName: k,
			}

			if err := visitJSON(elementsCopy, v, visit); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, v := range value {
			elementsCopy := make([]PathElement, len(elements)+1)
			copy(elementsCopy, elements)
			elementsCopy[len(elements)] = PathElement{
				Kind:       ArrayIndex,
				ArrayIndex: i,
			}

			if err := visitJSON(elementsCopy, v, visit); err != nil {
				return err
			}
		}
	}

	return nil
}

func hashData(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

func hashJSON(input interface{}) ([]byte, error) {
	marshalled, err := canonicaljson.Marshal(input)
	if err != nil {
		return nil, err
	}

	return hashData(marshalled), nil
}

func hexlify(b []byte) string {
	return fmt.Sprintf("%x", b)
}

func formatPath(elements []PathElement, stripArrayIndex bool) string {
	var rv string

	for _, e := range elements {
		switch e.Kind {
		case ObjectField:
			// TODO see if we need to escape the field name
			rv += "." + e.FieldName
		case ArrayIndex:
			if !stripArrayIndex {
				rv += fmt.Sprintf("[%d]", e.ArrayIndex)
			} else {
				rv += "[]"
			}
		}
	}

	return rv
}

func isAtomicJSONValue(value interface{}) bool {
	switch value.(type) {
	case string, float64, bool, nil:
		return true
	default:
		return false
	}
}

var (
	errMultiline     = errors.New("serialized record contains newline")
	errTooLong       = errors.New("serialized record too long")
	errNotObject     = errors.New("record is not a JSON object on the top-level")
	errTooManyFields = errors.New("record has too many fields")
)

type badJSONError struct {
	err error
}

func (b badJSONError) Error() string {
	return fmt.Sprintf("serialized record is invalid JSON: %s", b.err)
}

type canonicalizationError struct {
	err error
}

func (b canonicalizationError) Error() string {
	return fmt.Sprintf("serialized record could not be canonicalized: %s", b.err)
}

func (f *Flattener) FlattenJSON(recordData []byte) (*Record, error) {
	line := strings.TrimSpace(string(recordData))

	if strings.Count(line, "\n") > 0 {
		return nil, errMultiline
	}

	if len(line) > f.MaxSerializedLength {
		return nil, errTooLong
	}

	var unmarshalled interface{}
	if err := json.Unmarshal([]byte(line), &unmarshalled); err != nil {
		return nil, badJSONError{err}
	}

	canonicalForm, err := canonicaljson.Marshal(unmarshalled)
	if err != nil {
		return nil, canonicalizationError{err}
	}

	if len(canonicalForm) > f.MaxSerializedLength {
		return nil, errTooLong
	}

	unmarshalledObj, ok := unmarshalled.(map[string]interface{})
	if !ok {
		return nil, errNotObject
	}

	recordHash := hashData(canonicalForm)

	fieldValues := map[string][][]byte{}
	fieldsPresent := map[string]bool{}

	if err := visitJSON(nil, unmarshalledObj, func(elements []PathElement, value interface{}) (bool, error) {
		if len(elements) == 0 {
			return true, nil
		}
		key := formatPath(elements, true)

		fieldsPresent[key] = true
		if len(fieldsPresent) > f.MaxTotalFields {
			return false, errTooManyFields
		}

		switch value := value.(type) {
		case map[string]interface{}:
			if len(value) > f.MaxExploredObjectElements {
				return false, nil
			}
		case []interface{}:
			if len(value) > f.MaxExploredObjectElements {
				return false, nil
			}
		default:
			marshalledValue, err := canonicaljson.Marshal(value)
			if err != nil {
				return false, err
			}
			if len(marshalledValue) > f.MaxCapturedValueLength {
				return false, nil
			}

			fieldValues[key] = append(fieldValues[key], marshalledValue)
		}

		return true, nil
	}); err != nil {
		return nil, err
	}

	// TODO extract UUID from record, only generate a new one as a last resort
	recordUUID := uuid.NewString()

	// TODO extract timestamp from record, only use current time as a last resort
	recordTimestamp := time.Now()

	var fieldNames []string
	for k := range fieldsPresent {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	return &Record{
		RecordID:      recordUUID,
		Timestamp:     recordTimestamp,
		Hash:          hexlify(recordHash),
		FieldValues:   fieldValues,
		Fields:        fieldNames,
		CanonicalJSON: string(canonicalForm),
	}, nil
}

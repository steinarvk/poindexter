package flatten

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	canonicaljson "github.com/gibson042/canonicaljson-go"
	"github.com/google/uuid"
)

var (
	errRecordHasNoID        = errors.New("record has no ID")
	errRecordHasNoTimestamp = errors.New("record has no timestamp")
)

var (
	noExploreFieldName = "_noindex"
)

type falseFriend struct {
	badFieldName    string
	actualFieldName string
}

var falseFriends = []falseFriend{
	{"supersedes_uuid", "supersedes_id"},
	{"supersedes", "supersedes_id"},
	{"locked_until_time", "locked_until"},
	{"locked_until_timestamp", "locked_until"},
}

var validIDFieldNames = []string{
	"record_id",
	"record_uuid",
	"event_id",
	"event_uuid",
	"id",
	"uuid",
}

var validTimestampFieldNames = []string{
	"timestamp",
	"record_timestamp",
	"event_timestamp",
	"record_time",
	"event_time",
	"time",
}

var supersedesFieldName string = "supersedes_id"
var lockedUntilFieldName string = "locked_until"

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
	AcceptMissingID           bool
	AcceptMissingTimestamp    bool
	IgnoreNoIndex             bool
}

type Record struct {
	RecordUUID    uuid.UUID
	Timestamp     time.Time
	Hash          string
	FieldValues   map[string][][]byte
	Fields        []string
	CanonicalJSON string
	ShapeHash     string

	SupersedesUUID *uuid.UUID
	LockedUntil    *time.Time
}

func interpretFloatAsTimestamp(value float64) (time.Time, error) {
	multipliers := []float64{
		1.0,          // seconds
		1000.0,       // milliseconds
		1000000.0,    // microseconds
		1000000000.0, // nanoseconds
	}
	minReasonableYear := 2000
	maxReasonableYear := 2100
	for _, multiplier := range multipliers {
		seconds := int64(math.Floor(value / multiplier))
		fractionalSeconds := value - float64(seconds)*multiplier
		nanoseconds := int64(fractionalSeconds * 1e9)
		t := time.Unix(seconds, nanoseconds)
		if t.Year() >= minReasonableYear && t.Year() <= maxReasonableYear {
			return t, nil
		}
	}
	return time.Time{}, errors.New("invalid timestamp")
}

func interpretIntAsTimestamp(value int64) (time.Time, error) {
	multipliers := []int64{
		1,          // seconds
		1000,       // milliseconds
		1000000,    // microseconds
		1000000000, // nanoseconds
	}
	minReasonableYear := 2000
	maxReasonableYear := 2100
	for _, multiplier := range multipliers {
		t := time.Unix(value/multiplier, value%multiplier)
		if t.Year() >= minReasonableYear && t.Year() <= maxReasonableYear {
			return t, nil
		}
	}
	return time.Time{}, errors.New("invalid timestamp")
}

func interpretTimestamp(value interface{}) (time.Time, error) {
	allowedStringFormats := []string{
		time.RFC3339Nano,
		time.RFC3339,
	}

	switch value := value.(type) {
	case string:
		parsedInt, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return interpretIntAsTimestamp(parsedInt)
		}

		for _, format := range allowedStringFormats {
			t, err := time.Parse(format, value)
			if err == nil {
				return t, nil
			}
		}

		return time.Time{}, errors.New("invalid timestamp")
	case float64:
		return interpretFloatAsTimestamp(value)
	default:
		return time.Time{}, errors.New("invalid timestamp")
	}

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

func hashSortedFieldNames(fieldNames []string) string {
	h := sha256.New()
	for _, x := range fieldNames {
		h.Write([]byte(x))
		h.Write([]byte{'\n'})
	}
	return hexlify(h.Sum(nil))
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

	return f.FlattenObject(unmarshalled)
}

func (f *Flattener) flattenObjectToFields(unmarshalled interface{}) (map[string][][]byte, map[string]bool, error) {
	fieldValues := map[string][][]byte{}
	fieldsPresent := map[string]bool{}

	if err := visitJSON(nil, unmarshalled, func(elements []PathElement, value interface{}) (bool, error) {
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
			if f.IgnoreNoIndex {
				if noIndexValue, ok := value[noExploreFieldName]; ok {
					noIndexBool, ok := noIndexValue.(bool)
					if ok && noIndexBool {
						newKey := key + "." + noExploreFieldName
						fieldsPresent[newKey] = true
						fieldValues[newKey] = [][]byte{
							[]byte("true"),
						}
						return false, nil
					}
				}
			}

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
		return nil, nil, err
	}

	return fieldValues, fieldsPresent, nil
}

func (f *Flattener) FlattenObjectToFields(unmarshalled interface{}) (map[string][][]byte, error) {
	values, _, err := f.flattenObjectToFields(unmarshalled)
	return values, err
}

func (f *Flattener) FlattenObject(unmarshalled interface{}) (*Record, error) {
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

	fieldValues, fieldsPresent, err := f.flattenObjectToFields(unmarshalledObj)
	if err != nil {
		return nil, err
	}

	for _, ff := range falseFriends {
		if _, ok := unmarshalledObj[ff.badFieldName]; ok {
			return nil, fmt.Errorf("forbidden top-level field %q (the intent was probably %q)", ff.badFieldName, ff.actualFieldName)
		}
	}

	recordHash := hashData(canonicalForm)

	var recordID string
	for _, idFieldName := range validIDFieldNames {
		value, ok := unmarshalledObj[idFieldName]
		if ok {
			valueString, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("invalid ID field %q: not a string", idFieldName)
			}
			if ok {
				recordID = valueString
				break
			}
		}
	}
	var recordUUID uuid.UUID
	if recordID != "" {
		parsed, err := uuid.Parse(recordID)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID in record: %w", err)
		}
		recordUUID = parsed
	} else {
		if !f.AcceptMissingID {
			return nil, errRecordHasNoID
		}
		recordUUID = uuid.New()
	}

	var timestampValue interface{}
	for _, timestampFieldName := range validTimestampFieldNames {
		value, ok := unmarshalledObj[timestampFieldName]
		if ok {
			timestampValue = value
			break
		}
	}
	var recordTimestamp time.Time
	if timestampValue != nil {
		t, err := interpretTimestamp(timestampValue)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp in record: %w", err)
		}
		recordTimestamp = t
	} else {
		if !f.AcceptMissingTimestamp {
			return nil, errRecordHasNoTimestamp
		}
		recordTimestamp = time.Now()
	}

	var supersedesUUID *uuid.UUID

	if supersedesIDString, ok := unmarshalledObj[supersedesFieldName].(string); ok {
		parsed, err := uuid.Parse(supersedesIDString)
		if err != nil {
			return nil, fmt.Errorf("invalid %q in record (value was %q), %w", supersedesFieldName, supersedesIDString, err)
		}
		supersedesUUID = &parsed
	}

	var lockedUntil *time.Time
	if lockedUntilString, ok := unmarshalledObj[lockedUntilFieldName].(string); ok {
		t, err := interpretTimestamp(lockedUntilString)
		if err != nil {
			return nil, fmt.Errorf("invalid %q in record (value was %q), %w", lockedUntilFieldName, lockedUntilString, err)
		}
		lockedUntil = &t
	}

	var fieldNames []string
	for k := range fieldsPresent {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	recordShape := hashSortedFieldNames(fieldNames)

	return &Record{
		RecordUUID:     recordUUID,
		Timestamp:      recordTimestamp,
		Hash:           hexlify(recordHash),
		FieldValues:    fieldValues,
		Fields:         fieldNames,
		CanonicalJSON:  string(canonicalForm),
		SupersedesUUID: supersedesUUID,
		LockedUntil:    lockedUntil,
		ShapeHash:      recordShape,
	}, nil
}

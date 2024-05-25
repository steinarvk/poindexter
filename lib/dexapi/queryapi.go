package dexapi

import "github.com/steinarvk/poindexter/lib/dexerror"

type QueryRecordsRequest struct {
	Query
}

type QueryRecordsResponse struct {
	RecordList
}

type LookupRecordResponse struct {
	RecordItem
}

type CheckBatchResponse struct {
	BatchStatus
}

type ErrorResponse struct {
	Error dexerror.PublicErrorDetail `json:"error"`
}

type CheckBatchesRequest struct {
	BatchNames []string `json:"batch_names"`
}

type BatchStatus struct {
	Namespace string `json:"namespace"`
	BatchName string `json:"batch_name"`
	Processed bool   `json:"processed"`
}

type CheckBatchesResponse struct {
	Batches []BatchStatus `json:"batches"`
}

type ValueResponse struct {
	Value interface{} `json:"value"`
	Count *int        `json:"count,omitempty"`
}

type FieldResponse struct {
	Field  string          `json:"field"`
	Type   string          `json:"type,omitempty"`
	Count  *int            `json:"count,omitempty"`
	Values []ValueResponse `json:"values"`
}

// some basic queries for fields and values use the
// basic Query request type, but a different URL,
// and for values queries, a field also specified
// in the URL.

type QueryFieldsResponse struct {
	Fields []FieldResponse `json:"fields"`
}

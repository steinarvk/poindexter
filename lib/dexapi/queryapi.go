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
	BatchName    string `json:"batch_name"`
	BatchPresent bool   `json:"batch_present"`
}

type ErrorResponse struct {
	Error dexerror.PublicErrorDetail `json:"error"`
}

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

type ErrorResponse struct {
	Error dexerror.PublicErrorDetail `json:"error"`
}

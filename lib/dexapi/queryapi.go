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

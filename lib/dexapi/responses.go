package dexapi

import "github.com/google/uuid"

type RecordMetadata struct {
	Namespace         string `json:"namespace"`
	RecordID          string `json:"record_id"`
	EntityID          string `json:"entity_id"`
	Timestamp         string `json:"timestamp"`
	TimestampUnixNano string `json:"timestamp_unix_nano"`
}

type RecordItem struct {
	RecordMetadata
	Record map[string]interface{} `json:"record"`
}

type RawRecordItem struct {
	RecordMetadata
	RawRecord []byte
}

type RecordList struct {
	Records []RecordItem `json:"records"`
}

type IngestionStatsResponse struct {
	NumProcessed      int `json:"num_records"`
	NumOk             int `json:"num_ok"`
	NumAlreadyPresent int `json:"num_already_present"`
	NumInserted       int `json:"num_inserted"`
	NumError          int `json:"num_error"`
}

type IngestionItemRangeStatus struct {
	Index        int    `json:"index"`
	Count        int    `json:"count"`
	Ok           bool   `json:"ok"`
	FirstUUID    string `json:"first_uuid,omitempty"`
	ErrorMessage string `json:"error,omitempty"`
}

type IngestionResponse struct {
	Namespace  string                     `json:"namespace"`
	BatchName  string                     `json:"batch_name,omitempty"`
	Stats      IngestionStatsResponse     `json:"stats"`
	ItemStatus []IngestionItemRangeStatus `json:"item_status"`
	AllOK      bool                       `json:"all_ok"`
}

type UpsertEntityResponse struct {
	EntityID           uuid.UUID  `json:"entity_id"`
	RecordID           uuid.UUID  `json:"record_id"`
	SupersededRecordID *uuid.UUID `json:"superseded_record_id,omitempty"`
	Created            bool       `json:"created"`
	Updated            bool       `json:"updated"`
}

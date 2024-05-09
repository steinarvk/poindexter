package dexapi

type RecordMetadata struct {
	Namespace         string `json:"namespace"`
	RecordID          string `json:"record_id"`
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

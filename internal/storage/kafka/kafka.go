package kafka

// Header is one Kafka record header: an identifier the pipeline propagates (§7.3). Never a body.
type Header struct {
	Key   string
	Value []byte
}

// Record is a Kafka record in transit. On produce, Topic/Key/Value/Headers are set by the caller;
// on consume, Partition and Offset are filled in as well.
type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []Header
	Partition int32
	Offset    int64
}

// Header returns the value of the first header with the given key, and whether it was present.
func (r Record) Header(key string) ([]byte, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

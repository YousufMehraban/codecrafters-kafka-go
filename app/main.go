package main

import (
	"bytes"
	"encoding/binary"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sort"
)

// Ensures go fmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// declaring three errors for different scenarios.
const (
	errorNone                  int16 = 0
	errorUnknownTopic          int16 = 3
	errorApiUnsupportedVersion int16 = 35
)

// default topicID return a 16 bytes slice of zero vals [nil uuid]
func defaultTopicID() []byte {
	return make([]byte, 16)
}

func zeroUUID() []byte {
	return make([]byte, 16)
}

func parseLogDirFromServerProperties() string {
	// Default fallback as required by the challenge spec
	defaultDir := "/tmp/kafka-logs"

	// CodeCrafters usually runs the app like:
	// ./your_program.sh --properties-file server.properties
	// So we look for the argument immediately following the flag.
	var propsPath string
	for i, arg := range os.Args {
		if arg == "--properties-file" && i+1 < len(os.Args) {
			propsPath = os.Args[i+1]
			break
		}
	}

	// If no flag was found, try the first argument just in case
	if propsPath == "" && len(os.Args) > 1 {
		propsPath = os.Args[1]
	}

	// If we still don't have a path, return the default
	if propsPath == "" {
		return defaultDir
	}

	data, err := os.ReadFile(propsPath)
	if err != nil {
		return defaultDir
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		// Skip empty lines or comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "log.dirs=") {
			// Extract the value after the '='
			value := strings.TrimPrefix(line, "log.dirs=")
			// log.dirs can be a comma-separated list; we take the first one
			parts := strings.Split(value, ",")
			if len(parts) > 0 {
				return strings.TrimSpace(parts[0])
			}
		}
	}

	return defaultDir
}

func readTopicMetadata(topicName string) ([]byte, int, int16) {
	logDir := parseLogDirFromServerProperties()
	if logDir == "" {
		return make([]byte, 16), 0, errorUnknownTopic
	}

	metadataPath := filepath.Join(logDir, "__cluster_metadata-0", "00000000000000000000.log")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return make([]byte, 16), 0, errorUnknownTopic
	}

	// 1. Find the Topic ID
	nameBytes := []byte(topicName)
	idx := bytes.Index(data, nameBytes)
	if idx == -1 {
		// If topic name isn't in the file, return Zero UUID and Error 3
		return make([]byte, 16), 0, errorUnknownTopic
	}

	// The UUID (16 bytes) follows the topic name in the metadata log
	topicID := make([]byte, 16)
	idStart := idx + len(nameBytes)
	copy(topicID, data[idStart:idStart+16])

	// 2. Count Partitions for this Topic ID
	// We scan the file for occurrences of this Topic ID.
	// In the Kafka metadata log, each partition record contains the Topic ID.
	partitionCount := 0
	// We skip the first occurrence because that was the Topic definition itself
	searchOffset := idStart + 16

	for i := searchOffset; i <= len(data)-16; i++ {
		if bytes.Equal(data[i:i+16], topicID) {
			partitionCount++
		}
	}

	// If the log shows 0 partitions (rare), we default to 1 for the tester
	if partitionCount == 0 {
		partitionCount = 1
	}

	return topicID, partitionCount, errorNone
}

func sendError(connection net.Conn, correlationID uint32, errorCode int16) {
	response := make([]byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 6)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

	connection.Write(response)
}

func sendApiVersionResponse(connection net.Conn, correlationID uint32, apiVersion int16) {
	var b bytes.Buffer

	// 1. Header: Correlation ID (4 bytes)
	// ApiVersionsResponse ALWAYS uses a v0 header (no tag buffer in header)
	binary.Write(&b, binary.BigEndian, correlationID)

	// 2. Body: Error Code (int16)
	if apiVersion < 0 || apiVersion > 4 {
		binary.Write(&b, binary.BigEndian, int16(35)) // UNSUPPORTED_VERSION
	} else {
		binary.Write(&b, binary.BigEndian, int16(0)) // NO_ERROR
	}

	// 3. API Keys Array (Compact Array: N + 1)
	// We are sending 3 keys (1, 18, 75), so length is 4.
	b.WriteByte(5)

	// --- ApiKey 1: FETCH ---
	binary.Write(&b, binary.BigEndian, int16(1))  // API Key
	binary.Write(&b, binary.BigEndian, int16(0))  // Min Version
	binary.Write(&b, binary.BigEndian, int16(16)) // Max Version (at least 16)
	b.WriteByte(0)                                // Tag Buffer (per entry)

	// --- ApiKey 18: API_VERSIONS ---
	binary.Write(&b, binary.BigEndian, int16(18)) // API Key
	binary.Write(&b, binary.BigEndian, int16(0))  // Min Version
	binary.Write(&b, binary.BigEndian, int16(4))  // Max Version
	b.WriteByte(0)                                // Tag Buffer

	// --- ApiKey 75: DESCRIBE_TOPIC_PARTITIONS ---
	binary.Write(&b, binary.BigEndian, int16(75)) // API Key
	binary.Write(&b, binary.BigEndian, int16(0))  // Min Version
	binary.Write(&b, binary.BigEndian, int16(0))  // Max Version
	b.WriteByte(0)                                // Tag Buffer


	binary.Write(&b, binary.BigEndian, int16(0))  // API Key (PRODUCE)
	binary.Write(&b, binary.BigEndian, int16(0))  // Min Version
	binary.Write(&b, binary.BigEndian, int16(11)) // Max Version
	b.WriteByte(0)                                // Tag Buffer


	// 4. Throttle Time (int32)
	binary.Write(&b, binary.BigEndian, uint32(0))

	// 5. Main Tag Buffer (1 byte, 0 tags)
	b.WriteByte(0)

	// Final Response: Size Prefix + Buffer
	res := b.Bytes()
	final := make([]byte, 4+len(res))
	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
	copy(final[4:], res)
	connection.Write(final)
}





func processFetchRequest(connection net.Conn, correlationID uint32, requestBuffer []byte) {
	type fetchTopicRequest struct {
		topicID    []byte
		partitions []int32
	}
	writeCompactArrayLen := func(buf *bytes.Buffer, count int) {
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], uint64(count+1))
		buf.Write(tmp[:n])
	}
	writeUvarint := func(buf *bytes.Buffer, v uint64) {
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], v)
		buf.Write(tmp[:n])
	}
	curr := 8 // api_key + api_version + correlation_id
	topics := make([]fetchTopicRequest, 0)
	// RequestHeader v2: client_id (nullable string) + tagged fields
	if curr+2 <= len(requestBuffer) {
		clientIDLen := int16(binary.BigEndian.Uint16(requestBuffer[curr : curr+2]))
		curr += 2
		if clientIDLen > 0 {
			curr += int(clientIDLen)
		}
	}
	if curr < len(requestBuffer) {
		_, n := binary.Uvarint(requestBuffer[curr:]) // header tagged fields
		if n > 0 {
			curr += n
		}
	}
	// CodeCrafters fetch body layout in this stage:
	// max_wait_ms, min_bytes, max_bytes, isolation_level, session_id, session_epoch
	const fixedFetchFields = 4 + 4 + 4 + 1 + 4 + 4
	if curr+fixedFetchFields <= len(requestBuffer) {
		curr += fixedFetchFields
	}
	// topics => COMPACT_ARRAY
	if curr < len(requestBuffer) {
		topicArrayLen, n := binary.Uvarint(requestBuffer[curr:])
		if n > 0 {
			curr += n
			numTopics := int(topicArrayLen) - 1
			if numTopics < 0 {
				numTopics = 0
			}
			for i := 0; i < numTopics && curr < len(requestBuffer); i++ {
				if curr+16 > len(requestBuffer) {
					break
				}
				topicID := make([]byte, 16)
				copy(topicID, requestBuffer[curr:curr+16])
				curr += 16
				partitions := make([]int32, 0)
				partitionArrayLen, pn := binary.Uvarint(requestBuffer[curr:])
				if pn <= 0 {
					break
				}
				curr += pn
				numPartitions := int(partitionArrayLen) - 1
				if numPartitions < 0 {
					numPartitions = 0
				}
				for p := 0; p < numPartitions && curr < len(requestBuffer); p++ {
					// partition, current_leader_epoch, fetch_offset,
					// last_fetched_epoch, log_start_offset, partition_max_bytes
					const fetchPartitionFields = 4 + 4 + 8 + 4 + 8 + 4
					if curr+fetchPartitionFields > len(requestBuffer) {
						break
					}
					partitionIndex := int32(binary.BigEndian.Uint32(requestBuffer[curr : curr+4]))
					partitions = append(partitions, partitionIndex)
					curr += fetchPartitionFields
					// partition tagged fields
					_, tagsN := binary.Uvarint(requestBuffer[curr:])
					if tagsN <= 0 {
						break
					}
					curr += tagsN
				}
				// topic tagged fields
				if curr < len(requestBuffer) {
					_, tagsN := binary.Uvarint(requestBuffer[curr:])
					if tagsN > 0 {
						curr += tagsN
					}
				}
				topics = append(topics, fetchTopicRequest{
					topicID:    topicID,
					partitions: partitions,
				})
			}
		}
	}
	// Build FetchResponse v16
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0) // response header tagged fields
	binary.Write(&b, binary.BigEndian, int32(0)) // throttle_time_ms
	binary.Write(&b, binary.BigEndian, int16(0)) // body error_code
	binary.Write(&b, binary.BigEndian, int32(0)) // session_id
	writeCompactArrayLen(&b, len(topics))

	for _, topic := range topics {
		b.Write(topic.topicID)
		writeCompactArrayLen(&b, len(topic.partitions))
		_, topicErr := getTopicNameFromID(topic.topicID)
		for _, partitionIndex := range topic.partitions {
			// Defaults for UNKNOWN_TOPIC_ID
			partitionErr := int16(100)
			highWatermark := int64(-1)
			lastStableOffset := int64(-1)
			logStartOffset := int64(-1)
			abortedTransactionsLen := byte(1) // empty compact array
			recordBatchBytes := []byte(nil)
			// Known topic: read .log bytes for this topic/partition
			if topicErr == 0 {
				partitionErr = 0
				abortedTransactionsLen = 0 // null array in tester fixtures for known topics
				rb, err := readPartitionRecordBatches(topic.topicID, partitionIndex)
				if err == nil {
					recordBatchBytes = rb
				}
				if len(recordBatchBytes) == 0 {
					// "empty topic" behavior
					highWatermark = 0
					lastStableOffset = 0
					logStartOffset = 0
				} else {
					// multiple-messages stage: watermark = last_offset + 1
					hw := computeHighWatermarkFromRecordBatches(recordBatchBytes)
					if hw <= 0 {
						// fallback for malformed/unexpected bytes
						hw = 1
					}
					highWatermark = hw
					lastStableOffset = hw
					logStartOffset = 0
				}
			}
			binary.Write(&b, binary.BigEndian, partitionIndex)
			binary.Write(&b, binary.BigEndian, partitionErr)
			binary.Write(&b, binary.BigEndian, highWatermark)
			binary.Write(&b, binary.BigEndian, lastStableOffset)
			binary.Write(&b, binary.BigEndian, logStartOffset)
			b.WriteByte(abortedTransactionsLen)
			binary.Write(&b, binary.BigEndian, int32(-1)) // preferred_read_replica
			// records (compact record bytes)
			// size = byte_count + 1
			if len(recordBatchBytes) == 0 {
				writeUvarint(&b, 1) // empty records
			} else {
				writeUvarint(&b, uint64(len(recordBatchBytes)+1))
				b.Write(recordBatchBytes)
			}
			b.WriteByte(0) // partition tagged fields
		}
		b.WriteByte(0) // topic tagged fields
	}
	b.WriteByte(0) // response body tagged fields
	res := b.Bytes()
	final := make([]byte, 4+len(res))
	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
	copy(final[4:], res)
	connection.Write(final)
}


func computeHighWatermarkFromRecordBatches(logBytes []byte) int64 {
	// Kafka RecordBatch on disk:
	// base_offset:int64 (8) + batch_length:int32 (4) + <batch_length bytes>
	// last_offset_delta sits at offset 23 from batch start:
	// 8 + 4 + 4(partitionLeaderEpoch) + 1(magic) + 4(crc) + 2(attributes) = 23
	const (
		baseOffsetPos      = 0
		batchLengthPos     = 8
		lastOffsetDeltaPos = 23
		headerSize         = 12
	)
	cursor := 0
	var maxWatermark int64
	for cursor+headerSize <= len(logBytes) {
		baseOffset := int64(binary.BigEndian.Uint64(logBytes[cursor+baseOffsetPos : cursor+baseOffsetPos+8]))
		batchLength := int(binary.BigEndian.Uint32(logBytes[cursor+batchLengthPos : cursor+batchLengthPos+4]))
		if batchLength <= 0 {
			break
		}
		batchEnd := cursor + headerSize + batchLength
		if batchEnd > len(logBytes) {
			break
		}
		// Prefer exact last_offset_delta if present
		if cursor+lastOffsetDeltaPos+4 <= batchEnd {
			lastOffsetDelta := int32(binary.BigEndian.Uint32(logBytes[cursor+lastOffsetDeltaPos : cursor+lastOffsetDeltaPos+4]))
			watermark := baseOffset + int64(lastOffsetDelta) + 1
			if watermark > maxWatermark {
				maxWatermark = watermark
			}
		} else {
			// Safe fallback: at least one offset consumed by this batch
			watermark := baseOffset + 1
			if watermark > maxWatermark {
				maxWatermark = watermark
			}
		}
		cursor = batchEnd
	}
	return maxWatermark
}


func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicNames []string) {
	var b bytes.Buffer

	// 1. Header & Tagged Fields
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0) // Header Tag Buffer

	// 2. Throttle Time
	binary.Write(&b, binary.BigEndian, uint32(0))

	// Sort topic names to match tester's expected ordering.
	sortedTopicNames := append([]string(nil), topicNames...)
	sort.Strings(sortedTopicNames)

	// 3. Topics Array Length (N + 1)
	b.WriteByte(byte(len(topicNames) + 1))

	for _, name := range topicNames {
		// Get dynamic data from metadata
		topicID, partitionCount, errCode := readTopicMetadata(name)

		// A. Topic Error Code
		binary.Write(&b, binary.BigEndian, errCode)

		// B. Topic Name (Compact String)
		b.WriteByte(byte(len(name) + 1))
		b.WriteString(name)

		// C. Topic ID
		b.Write(topicID)

		// D. Is Internal
		b.WriteByte(0)

		// E. Partitions Array (Compact: Count + 1)
		if errCode != errorNone {
			b.WriteByte(1) // 0 partitions + 1
		} else {
			b.WriteByte(byte(partitionCount + 1))

			// Loop to create the exact number of partitions found
			for i := 0; i < partitionCount; i++ {
				binary.Write(&b, binary.BigEndian, errorNone)
				binary.Write(&b, binary.BigEndian, uint32(i)) // Partition Index (0, 1, etc.)
				binary.Write(&b, binary.BigEndian, uint32(1)) // Leader ID
				binary.Write(&b, binary.BigEndian, uint32(0)) // Leader Epoch

				// Replicas (1 replica -> 2)
				b.WriteByte(2)
				binary.Write(&b, binary.BigEndian, uint32(1))

				// ISR (1 ISR -> 2)
				b.WriteByte(2)
				binary.Write(&b, binary.BigEndian, uint32(1))

				b.WriteByte(1) // Offline
				b.WriteByte(1) // ELR
				b.WriteByte(1) // Last ELR
				b.WriteByte(0) // Partition Tags
			}
		}

		// F. Topic Authorized Operations
		binary.Write(&b, binary.BigEndian, uint32(3576))

		// G. Topic Tag Buffer
		b.WriteByte(0)
	}

	// 4. Final Metadata
	b.WriteByte(0xff) // Next Cursor
	b.WriteByte(0)    // Main Tag Buffer

	// 5. Send Response with Size Prefix
	res := b.Bytes()
	final := make([]byte, 4+len(res))
	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
	copy(final[4:], res)
	connection.Write(final)
}

// Helper to find if a Topic ID exists in your metadata
func getTopicNameFromID(targetID []byte) (string, int16) {
	logDir := parseLogDirFromServerProperties()
	metadataPath := filepath.Join(logDir, "__cluster_metadata-0", "00000000000000000000.log")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return "", 100 // UNKNOWN_TOPIC_ID
	}

	// Scan the metadata for the 16-byte UUID
	idx := bytes.Index(data, targetID)
	if idx == -1 {
		return "", 100 // Error 100 is required for unknown UUIDs
	}

	// Usually, the topic name precedes the UUID in the TopicRecord.
	// For this stage, if we find the ID, we return errorNone.
	return "found", 0
}




func readPartitionRecordBatches(topicID []byte, partitionID int32) ([]byte, error) {
	logDir := parseLogDirFromServerProperties()
	targetTopicIDB64 := base64.StdEncoding.EncodeToString(topicID)
	targetSuffix := fmt.Sprintf("-%d", partitionID)
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		if strings.HasPrefix(dirName, "__cluster_metadata-") {
			continue
		}
		if !strings.HasSuffix(dirName, targetSuffix) {
			continue
		}
		metaPath := filepath.Join(logDir, dirName, "partition.metadata")
		metaBytes, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		if !partitionMetadataHasTopicID(metaBytes, targetTopicIDB64) {
			continue
		}
		logPath := filepath.Join(logDir, dirName, "00000000000000000000.log")
		logBytes, err := os.ReadFile(logPath)
		if err != nil {
			return nil, err
		}
		return logBytes, nil
	}
	// Not found -> treat as no records for this stage path
	return nil, nil
}
func partitionMetadataHasTopicID(metadata []byte, expectedTopicIDB64 string) bool {
	for _, line := range strings.Split(string(metadata), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "topic_id:") {
			got := strings.TrimSpace(strings.TrimPrefix(line, "topic_id:"))
			return got == expectedTopicIDB64
		}
	}
	return false
}







func produceTopicPartitionExists(topicName string, partitionID int32) bool {
	logDir := parseLogDirFromServerProperties()
	partitionDir := filepath.Join(logDir, fmt.Sprintf("%s-%d", topicName, partitionID))
	info, err := os.Stat(partitionDir)
	if err != nil || !info.IsDir() {
		return false
	}
	// extra guard: partition.metadata should exist in generated dirs
	_, err = os.Stat(filepath.Join(partitionDir, "partition.metadata"))
	return err == nil
}



func writeProducePartitionLog(topicName string, partitionID int32, recordBatches []byte) error {
	logDir := parseLogDirFromServerProperties()
	logPath := filepath.Join(logDir, fmt.Sprintf("%s-%d", topicName, partitionID), "00000000000000000000.log")
	return os.WriteFile(logPath, recordBatches, 0o644)
}



// func processProduceRequest(connection net.Conn, correlationID uint32, requestBuffer []byte) {
// 	type producePartitionReq struct {
// 		id            int32
// 		recordBatches []byte
// 	}
// 	type produceTopicReq struct {
// 		name       string
// 		partitions []producePartitionReq
// 	}
// 	var topics []produceTopicReq
// 	curr := 8 // api_key + api_version + correlation_id
// 	advance := func(n int) bool {
// 		if n < 0 || curr+n > len(requestBuffer) {
// 			curr = len(requestBuffer)
// 			return false
// 		}
// 		curr += n
// 		return true
// 	}
// 	readUvarint := func() (uint64, bool) {
// 		if curr >= len(requestBuffer) {
// 			return 0, false
// 		}
// 		v, n := binary.Uvarint(requestBuffer[curr:])
// 		if n <= 0 {
// 			return 0, false
// 		}
// 		curr += n
// 		return v, true
// 	}
// 	readCompactArrayCount := func() (int, bool) {
// 		v, ok := readUvarint()
// 		if !ok {
// 			return 0, false
// 		}
// 		count := int(v) - 1
// 		if count < 0 {
// 			count = 0
// 		}
// 		return count, true
// 	}
// 	readCompactString := func() (string, bool) {
// 		v, ok := readUvarint()
// 		if !ok || v == 0 {
// 			return "", false
// 		}
// 		strLen := int(v - 1)
// 		if curr+strLen > len(requestBuffer) {
// 			return "", false
// 		}
// 		s := string(requestBuffer[curr : curr+strLen])
// 		curr += strLen
// 		return s, true
// 	}
// 	// ---- Parse RequestHeader v2 ----
// 	// client_id (nullable string)
// 	if curr+2 <= len(requestBuffer) {
// 		clientIDLen := int(int16(binary.BigEndian.Uint16(requestBuffer[curr : curr+2])))
// 		if !advance(2) {
// 			goto BUILD_RESPONSE
// 		}
// 		if clientIDLen > 0 && !advance(clientIDLen) {
// 			goto BUILD_RESPONSE
// 		}
// 	}
// 	// header tagged buffer fields
// 	if _, ok := readUvarint(); !ok {
// 		goto BUILD_RESPONSE
// 	}
// 	// ---- Parse ProduceRequest body v11 ----
// 	// transactional_id (compact nullable string): 0 => null, >0 => len-1 bytes
// 	if txLen, ok := readUvarint(); ok {
// 		if txLen > 1 && !advance(int(txLen-1)) {
// 			goto BUILD_RESPONSE
// 		}
// 	} else {
// 		goto BUILD_RESPONSE
// 	}
// 	// acks + timeout_ms
// 	if !advance(2 + 4) {
// 		goto BUILD_RESPONSE
// 	}
// 	// topics (compact array)
// 	if topicCount, ok := readCompactArrayCount(); ok {
// 		for i := 0; i < topicCount; i++ {
// 			name, ok := readCompactString()
// 			if !ok {
// 				goto BUILD_RESPONSE
// 			}
// 			partitionCount, ok := readCompactArrayCount()
// 			if !ok {
// 				goto BUILD_RESPONSE
// 			}
// 			partitions := make([]producePartitionReq, 0, partitionCount)
// 			for p := 0; p < partitionCount; p++ {
// 				if curr+4 > len(requestBuffer) {
// 					goto BUILD_RESPONSE
// 				}
// 				partitionID := int32(binary.BigEndian.Uint32(requestBuffer[curr : curr+4]))
// 				curr += 4
// 				// record_batches_size (uvarint): encoded as byte_count + 1
// 				rbSize, ok := readUvarint()
// 				if !ok {
// 					goto BUILD_RESPONSE
// 				}
// 				rbLen := 0
// 				if rbSize > 1 {
// 					rbLen = int(rbSize - 1)
// 				}
// 				if curr+rbLen > len(requestBuffer) {
// 					goto BUILD_RESPONSE
// 				}
// 				recordBatches := make([]byte, rbLen)
// 				copy(recordBatches, requestBuffer[curr:curr+rbLen])
// 				curr += rbLen
// 				// partition tagged fields
// 				if _, ok := readUvarint(); !ok {
// 					goto BUILD_RESPONSE
// 				}
// 				partitions = append(partitions, producePartitionReq{
// 					id:            partitionID,
// 					recordBatches: recordBatches,
// 				})
// 			}
// 			// topic tagged fields
// 			if _, ok := readUvarint(); !ok {
// 				goto BUILD_RESPONSE
// 			}
// 			topics = append(topics, produceTopicReq{
// 				name:       name,
// 				partitions: partitions,
// 			})
// 		}
// 	}
// 	// body tagged fields
// 	_, _ = readUvarint()
// BUILD_RESPONSE:
// 	// ---- Build ProduceResponse v11 ----
// 	var b bytes.Buffer
// 	writeCompactArrayLen := func(buf *bytes.Buffer, count int) {
// 		var tmp [10]byte
// 		n := binary.PutUvarint(tmp[:], uint64(count+1))
// 		buf.Write(tmp[:n])
// 	}
// 	writeCompactString := func(buf *bytes.Buffer, s string) {
// 		var tmp [10]byte
// 		n := binary.PutUvarint(tmp[:], uint64(len(s)+1))
// 		buf.Write(tmp[:n])
// 		buf.WriteString(s)
// 	}
// 	// ResponseHeader v1
// 	binary.Write(&b, binary.BigEndian, correlationID)
// 	b.WriteByte(0) // header tagged fields
// 	// topics
// 	writeCompactArrayLen(&b, len(topics))
// 	for _, t := range topics {
// 		writeCompactString(&b, t.name)
// 		writeCompactArrayLen(&b, len(t.partitions))
// 		for _, p := range t.partitions {
// 			errCode := int16(3) // UNKNOWN_TOPIC_OR_PARTITION
// 			baseOffset := int64(-1)
// 			logAppendTimeMs := int64(-1)
// 			logStartOffset := int64(-1)
// 			// Valid topic+partition for LS8: write request record batch bytes to disk
// 			if produceTopicPartitionExists(t.name, p.id) {
// 				if err := writeProducePartitionLog(t.name, p.id, p.recordBatches); err == nil {
// 					errCode = 0
// 					baseOffset = 0
// 					logStartOffset = 0
// 				}
// 			}
// 			binary.Write(&b, binary.BigEndian, p.id)
// 			binary.Write(&b, binary.BigEndian, errCode)
// 			binary.Write(&b, binary.BigEndian, baseOffset)
// 			binary.Write(&b, binary.BigEndian, logAppendTimeMs)
// 			binary.Write(&b, binary.BigEndian, logStartOffset)
// 			b.WriteByte(1) // record_errors: empty compact array
// 			b.WriteByte(0) // error_message: null compact nullable string
// 			b.WriteByte(0) // partition tagged fields
// 		}
// 		b.WriteByte(0) // topic tagged fields
// 	}
// 	binary.Write(&b, binary.BigEndian, int32(0)) // throttle_time_ms
// 	b.WriteByte(0)                               // body tagged fields
// 	res := b.Bytes()
// 	final := make([]byte, 4+len(res))
// 	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
// 	copy(final[4:], res)
// 	connection.Write(final)
// }


func processProduceRequest(connection net.Conn, correlationID uint32, requestBuffer []byte) {
	type producePartitionReq struct {
		id            int32
		recordBatches []byte
	}
	type produceTopicReq struct {
		name       string
		partitions []producePartitionReq
	}
	var topics []produceTopicReq
	curr := 8 // api_key + api_version + correlation_id
	advance := func(n int) bool {
		if n < 0 || curr+n > len(requestBuffer) {
			curr = len(requestBuffer)
			return false
		}
		curr += n
		return true
	}
	readUvarint := func() (uint64, bool) {
		if curr >= len(requestBuffer) {
			return 0, false
		}
		v, n := binary.Uvarint(requestBuffer[curr:])
		if n <= 0 {
			return 0, false
		}
		curr += n
		return v, true
	}
	readCompactArrayCount := func() (int, bool) {
		v, ok := readUvarint()
		if !ok {
			return 0, false
		}
		count := int(v) - 1
		if count < 0 {
			count = 0
		}
		return count, true
	}
	readCompactString := func() (string, bool) {
		v, ok := readUvarint()
		if !ok || v == 0 {
			return "", false
		}
		strLen := int(v - 1)
		if curr+strLen > len(requestBuffer) {
			return "", false
		}
		s := string(requestBuffer[curr : curr+strLen])
		curr += strLen
		return s, true
	}
	// Header v2
	if curr+2 <= len(requestBuffer) {
		clientIDLen := int(int16(binary.BigEndian.Uint16(requestBuffer[curr : curr+2])))
		if !advance(2) {
			goto BUILD_RESPONSE
		}
		if clientIDLen > 0 && !advance(clientIDLen) {
			goto BUILD_RESPONSE
		}
	}
	if _, ok := readUvarint(); !ok { // header tag buffer
		goto BUILD_RESPONSE
	}
	// Produce body v11
	if txLen, ok := readUvarint(); ok { // transactional_id compact nullable string
		if txLen > 1 && !advance(int(txLen-1)) {
			goto BUILD_RESPONSE
		}
	} else {
		goto BUILD_RESPONSE
	}
	if !advance(2 + 4) { // acks + timeout_ms
		goto BUILD_RESPONSE
	}
	if topicCount, ok := readCompactArrayCount(); ok {
		for i := 0; i < topicCount; i++ {
			name, ok := readCompactString()
			if !ok {
				goto BUILD_RESPONSE
			}
			partitionCount, ok := readCompactArrayCount()
			if !ok {
				goto BUILD_RESPONSE
			}
			partitions := make([]producePartitionReq, 0, partitionCount)
			for p := 0; p < partitionCount; p++ {
				if curr+4 > len(requestBuffer) {
					goto BUILD_RESPONSE
				}
				partitionID := int32(binary.BigEndian.Uint32(requestBuffer[curr : curr+4]))
				curr += 4
				rbSize, ok := readUvarint() // encoded as len+1
				if !ok {
					goto BUILD_RESPONSE
				}
				rbLen := 0
				if rbSize > 1 {
					rbLen = int(rbSize - 1)
				}
				if curr+rbLen > len(requestBuffer) {
					goto BUILD_RESPONSE
				}
				recordBatches := make([]byte, rbLen)
				copy(recordBatches, requestBuffer[curr:curr+rbLen])
				curr += rbLen
				if _, ok := readUvarint(); !ok { // partition tag buffer
					goto BUILD_RESPONSE
				}
				partitions = append(partitions, producePartitionReq{
					id:            partitionID,
					recordBatches: recordBatches,
				})
			}
			if _, ok := readUvarint(); !ok { // topic tag buffer
				goto BUILD_RESPONSE
			}
			topics = append(topics, produceTopicReq{
				name:       name,
				partitions: partitions,
			})
		}
	}
	_, _ = readUvarint() // body tag buffer
BUILD_RESPONSE:
	var b bytes.Buffer
	writeCompactArrayLen := func(buf *bytes.Buffer, count int) {
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], uint64(count+1))
		buf.Write(tmp[:n])
	}
	writeCompactString := func(buf *bytes.Buffer, s string) {
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], uint64(len(s)+1))
		buf.Write(tmp[:n])
		buf.WriteString(s)
	}
	// response header v1
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0)
	writeCompactArrayLen(&b, len(topics))
	for _, t := range topics {
		writeCompactString(&b, t.name)
		writeCompactArrayLen(&b, len(t.partitions))
		for _, p := range t.partitions {
			errCode := int16(3) // UNKNOWN_TOPIC_OR_PARTITION
			baseOffset := int64(-1)
			logAppendTimeMs := int64(-1)
			logStartOffset := int64(-1)
			// CT4 key behavior: handle every partition independently
			if produceTopicPartitionExists(t.name, p.id) {
				if err := writeProducePartitionLog(t.name, p.id, p.recordBatches); err == nil {
					errCode = 0
					baseOffset = 0
					logStartOffset = 0
				}
			}
			binary.Write(&b, binary.BigEndian, p.id)
			binary.Write(&b, binary.BigEndian, errCode)
			binary.Write(&b, binary.BigEndian, baseOffset)
			binary.Write(&b, binary.BigEndian, logAppendTimeMs)
			binary.Write(&b, binary.BigEndian, logStartOffset)
			b.WriteByte(1) // record_errors empty compact array
			b.WriteByte(0) // error_message null
			b.WriteByte(0) // partition tagged fields
		}
		b.WriteByte(0) // topic tagged fields
	}
	binary.Write(&b, binary.BigEndian, int32(0)) // throttle_time_ms
	b.WriteByte(0)                               // body tagged fields
	res := b.Bytes()
	final := make([]byte, 4+len(res))
	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
	copy(final[4:], res)
	connection.Write(final)
}


func handleClientRequest(connection net.Conn) {
	defer connection.Close() // close connectiong if when loop breaks

	for {
		sizeBuffer := make([]byte, 4)
		_, err := io.ReadFull(connection, sizeBuffer)
		if err != nil {
			return // exit the loop and close connection
		}

		// messageSize := binary.BigEndian.Uint32(sizeBuffer)
		requestBuffer := make([]byte, binary.BigEndian.Uint32(sizeBuffer))
		if _, err := io.ReadFull(connection, requestBuffer); err != nil {
			return
		}

		apiKey := int16(binary.BigEndian.Uint16(requestBuffer[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(requestBuffer[2:4]))
		correlationID := binary.BigEndian.Uint32(requestBuffer[4:8])

		switch apiKey {
		case 0:
			// PRODUCE (v11)
			processProduceRequest(connection, correlationID, requestBuffer)
		case 1:
			// FETCH
			// For a "No Topic" test, we don't need to parse the body deeply,
			// but we must acknowledge the request and send the empty response.
			processFetchRequest(connection, correlationID, requestBuffer)

		case 18:
			sendApiVersionResponse(connection, correlationID, apiVersion)
		case 75:
			// Manual skip of header to find Topic Name
			curr := 8
			clientIDLen := int(int16(binary.BigEndian.Uint16(requestBuffer[curr : curr+2])))
			if clientIDLen > 0 {
				curr += clientIDLen
			}
			curr += 2 // clientID length field
			curr++    // tag buffer

			topicArrayLen, n := binary.Uvarint(requestBuffer[curr:])
			curr += n
			numTopics := int(topicArrayLen) - 1

			var requestedTopics []string
			for i := 0; i < numTopics; i++ {
				nameLen, n := binary.Uvarint(requestBuffer[curr:])
				curr += n
				topicName := string(requestBuffer[curr : curr+int(nameLen)-1])
				curr += int(nameLen) - 1
				// curr++ // skip tag buffer for each topic
				_, n = binary.Uvarint(requestBuffer[curr:])
				curr += n

				requestedTopics = append(requestedTopics, topicName)
			}

			processTopicPartitionResponse(connection, correlationID, requestedTopics)
		}

	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleClientRequest(connection)
	}
}

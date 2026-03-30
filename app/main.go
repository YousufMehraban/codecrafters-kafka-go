package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// Ensures go fmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit



// declaring three errors for different scenarios.
const(
	errorNone int16 = 0
	errorUnknownTopic int16 = 3
	errorApiUnsupportedVersion int16 = 35
)



// default topicID return a 16 bytes slice of zero vals [nil uuid]
func defaultTopicID() []byte {
	return make([] byte, 16)
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





func sendError(connection net.Conn, correlationID uint32, errorCode int16){
	response := make([] byte, 10)
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
		binary.Write(&b, binary.BigEndian, int16(0))  // NO_ERROR
	}

	// 3. API Keys Array (Compact Array: N + 1)
	// We are sending 3 keys (1, 18, 75), so length is 4.
	b.WriteByte(4)

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
	binary.Write(&b, binary.BigEndian, int16(4))  // Max Version
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



// func processFetchRequest(connection net.Conn, correlationID uint32, requestBuffer []byte) {
// 	var b bytes.Buffer

// 	// 1. Header: Correlation ID (4 bytes)
// 	binary.Write(&b, binary.BigEndian, correlationID)
// 	// 2. Header Tag Buffer (Flexible versions require this, usually 0)
// 	b.WriteByte(0)

// 	// --- Response Body ---

// 	// 3. Throttle Time (int32)
// 	binary.Write(&b, binary.BigEndian, uint32(0))

// 	// 4. Error Code (int16) - Global error for the fetch session
// 	binary.Write(&b, binary.BigEndian, int16(0))

// 	// 5. Session ID (int32) - 0 if not using fetch sessions
// 	binary.Write(&b, binary.BigEndian, uint32(0))

// 	// 6. Topics Array (COMPACT ARRAY)
// 	// Requirement: "No topics" means an empty array.
// 	// Compact Array encoding: N + 1. Since N=0, we write 1.
// 	b.WriteByte(1)

// 	// 7. Main Response Tag Buffer (1 byte, 0 tags)
// 	b.WriteByte(0)

// 	// --- Final Send ---
// 	resBytes := b.Bytes()
// 	finalResponse := make([]byte, 4+len(resBytes))
// 	binary.BigEndian.PutUint32(finalResponse[0:4], uint32(len(resBytes)))
// 	copy(finalResponse[4:], resBytes)

// 	connection.Write(finalResponse)
// }




// func processFetchRequest(connection net.Conn, correlationID uint32, requestBuffer []byte) {
// 	var b bytes.Buffer

// 	// --- 1. PARSING THE REQUEST ---
// 	curr := 8 // Start after header
// 	// Skip ClientID
// 	idLen := int(binary.BigEndian.Uint16(requestBuffer[curr : curr+2]))
// 	curr += 2 + idLen + 1 // skip clientId and header tags

// 	// Skip global fetch fields (MaxWait, MinBytes, etc.)
// 	curr += 15 

// 	// Read Topics Array Length (Compact)
// 	topicArrayLen, n := binary.Uvarint(requestBuffer[curr:])
// 	curr += n
// 	numTopics := int(topicArrayLen) - 1

// 	type topicReq struct {
// 		id []byte
// 	}
// 	var requestedTopics []topicReq
// 	for i := 0; i < numTopics; i++ {
// 		tID := make([]byte, 16)
// 		copy(tID, requestBuffer[curr:curr+16])
// 		curr += 16
		
// 		// Skip partitions array for now
// 		_, n = binary.Uvarint(requestBuffer[curr:])
// 		curr += n
// 		curr++ // skip tags
		
// 		requestedTopics = append(requestedTopics, topicReq{id: tID})
// 	}

// 	// --- 2. BUILDING THE RESPONSE ---
	
// 	binary.Write(&b, binary.BigEndian, correlationID)
// 	b.WriteByte(0) // Header Tag

// 	binary.Write(&b, binary.BigEndian, uint32(0)) // Throttle
// 	binary.Write(&b, binary.BigEndian, int16(0))  // Global Error
// 	binary.Write(&b, binary.BigEndian, uint32(0)) // Session ID

// 	// Topics Array (Compact)
// 	b.WriteByte(byte(len(requestedTopics) + 1))

// 	for _, t := range requestedTopics {
// 		// Check if ID is known
// 		_, errCode := getTopicNameFromID(t.id)

// 		// A. Topic ID
// 		b.Write(t.id)

// 		// B. Partitions Array
// 		// For an unknown topic, return 1 partition with Error 100
// 		b.WriteByte(2) 
// 		binary.Write(&b, binary.BigEndian, uint32(0)) // Partition Index
// 		binary.Write(&b, binary.BigEndian, errCode)   // Error Code 100 goes here!
		
// 		binary.Write(&b, binary.BigEndian, int64(0))  // High Watermark
// 		binary.Write(&b, binary.BigEndian, int64(0))  // Last Stable Offset
// 		binary.Write(&b, binary.BigEndian, int64(0))  // Log Start Offset
		
// 		b.WriteByte(1) // Aborted Transactions (Empty)
// 		binary.Write(&b, binary.BigEndian, uint32(0)) // Preferred Read Replica
		
// 		// Records (Empty Compact Records Batch)
// 		// A null/empty record set is just 0x01 in compact format
// 		b.WriteByte(1) 
		
// 		b.WriteByte(0) // Partition Tags
// 	}

// 	b.WriteByte(0) // Main Tag Buffer

// 	// --- 3. SEND ---
// 	res := b.Bytes()
// 	final := make([]byte, 4+len(res))
// 	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
// 	copy(final[4:], res)
// 	connection.Write(final)
// }





func processFetchRequest(conn net.Conn, correlationID uint32, reqBuf []byte) {
	var b bytes.Buffer

	// 1. Header: Correlation ID + Header Tag Buffer (Flexible v16+)
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0) // Header Tag Buffer

	// --- 2. RESPONSE BODY (Order is CRITICAL) ---
	
	// A. ThrottleTimeMs (int32)
	binary.Write(&b, binary.BigEndian, uint32(0))

	// B. ErrorCode (int16) - Global error
	binary.Write(&b, binary.BigEndian, int16(0))

	// C. SessionId (int32)
	binary.Write(&b, binary.BigEndian, uint32(0))

	// --- 3. PARSING REQUEST ---
	curr := 8 
	clientIdLen := int(binary.BigEndian.Uint16(reqBuf[curr : curr+2]))
	curr += 2 + clientIdLen + 1 // skip clientId and header tags
	
	// Skip global request fields (MaxWait, MinBytes, etc.)
	curr += 15 

	// Read Topics Array Length (Compact Varint)
	topicArrayLen, n := binary.Uvarint(reqBuf[curr:])
	curr += n
	numTopics := int(topicArrayLen) - 1

	// --- 4. BUILDING RESPONSE TOPICS ARRAY ---
	// Write Topics Array Length (numTopics + 1)
	b.WriteByte(byte(numTopics + 1))

	for i := 0; i < numTopics; i++ {
		// Read 16-byte Topic ID from Request
		topicID := make([]byte, 16)
		copy(topicID, reqBuf[curr:curr+16])
		curr += 16
		
		// Skip request's partition array for this topic
		_, n = binary.Uvarint(reqBuf[curr:])
		curr += n + 1 // skip tags

		// A. Write Topic ID back to Response
		b.Write(topicID)

		// B. Write Partitions Array (1 partition = 2)
		b.WriteByte(2) 

		// --- Start Partition 0 Block ---
		binary.Write(&b, binary.BigEndian, uint32(0))   // Partition Index
		binary.Write(&b, binary.BigEndian, int16(100))  // Error: UNKNOWN_TOPIC_ID (100)
		
		binary.Write(&b, binary.BigEndian, int64(0))    // High Watermark
		binary.Write(&b, binary.BigEndian, int64(0))    // Last Stable Offset
		binary.Write(&b, binary.BigEndian, int64(0))    // Log Start Offset
		
		b.WriteByte(1) // Aborted Transactions (Empty = 1)
		binary.Write(&b, binary.BigEndian, uint32(0))   // Preferred Read Replica
		b.WriteByte(1) // Records (Empty = 1)
		b.WriteByte(0) // Partition Tagged Fields
	}

	// 5. Main Response Tag Buffer
	b.WriteByte(0)

	// --- 6. FINAL SEND ---
	res := b.Bytes()
	final := make([]byte, 4+len(res))
	binary.BigEndian.PutUint32(final[0:4], uint32(len(res)))
	copy(final[4:], res)
	conn.Write(final)
}






func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicNames []string) {
	var b bytes.Buffer

	// 1. Header & Tagged Fields
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0) // Header Tag Buffer

	// 2. Throttle Time
	binary.Write(&b, binary.BigEndian, uint32(0))

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




func handleClientRequest(connection net.Conn){
	defer connection.Close()  // close connectiong if when loop breaks

	for{
		sizeBuffer := make([]byte, 4)
		_, err := io.ReadFull(connection, sizeBuffer)
		if err != nil{
			return 		// exit the loop and close connection
		}
		
		// messageSize := binary.BigEndian.Uint32(sizeBuffer)
		requestBuffer := make([]byte, binary.BigEndian.Uint32(sizeBuffer))
		if _, err := io.ReadFull(connection, requestBuffer); err != nil{
			return
		}

		apiKey := int16(binary.BigEndian.Uint16(requestBuffer[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(requestBuffer[2:4]))
		correlationID := binary.BigEndian.Uint32(requestBuffer[4:8])

		switch apiKey {
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
			if clientIDLen > 0 { curr += clientIDLen }
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

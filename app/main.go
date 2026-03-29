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

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit



// func sendResponse(connection net.Conn) error {
	
// 	const messageSize uint32 = 4
// 	const correlationID uint32 = 4
// 	byteSlice := make([] byte, 8)

// 	binary.BigEndian.PutUint32(byteSlice[0:4], messageSize)
// 	binary.BigEndian.PutUint32(byteSlice[4:8], correlationID)

// 	_, err := connection.Write(byteSlice)

// 	return err
// }

// const errorNone int16 = 0
// const errorApiUnsupportedVersion = 35

const(
	errorNone int16 = 0
	errorUnknownTopic int16 = 3
	errorApiUnsupportedVersion int16 = 35
)

// var emptyTopicID = make([]byte, 16)

// func defaultTopicID() []byte {
// 	id, err := hex.DecodeString("71a59a5189684f8b937ee6a0943b74ec")
// 	if err != nil || len(id) != 16 {
// 		return make([]byte, 16)
// 	}
// 	return id
// }

// default topicID return a 16 bytes slice of zero vals [nil uuid]
func defaultTopicID() []byte {
	return make([] byte, 16)
}



// func cloneBytes(in []byte) []byte {
// 	out := make([]byte, len(in))
// 	copy(out, in)
// 	return out
// }

// func parseLogDirFromServerProperties() string {
// 	if len(os.Args) < 2 {
// 		return ""
// 	}

// 	propsPath := os.Args[1]
// 	propsData, err := os.ReadFile(propsPath)
// 	if err != nil {
// 		return ""
// 	}

// 	for _, rawLine := range strings.Split(string(propsData), "\n") {
// 		line := strings.TrimSpace(rawLine)
// 		if line == "" || strings.HasPrefix(line, "#") {
// 			continue
// 		}
// 		if strings.HasPrefix(line, "log.dirs=") {
// 			val := strings.TrimSpace(strings.TrimPrefix(line, "log.dirs="))
// 			if val == "" {
// 				return ""
// 			}
// 			// Codecrafters uses a single log dir for this stage.
// 			parts := strings.Split(val, ",")
// 			return strings.TrimSpace(parts[0])
// 		}
// 	}

// 	return ""
// }


func parseLogDirectory() string {
	if len(os.Args) < 2 {
		// Fallback if no config file is provided
		return "/tmp/kafka-logs"
	}
	
	// CodeCrafters passes the properties file path as the first argument
	propsPath := os.Args[1]
	data, err := os.ReadFile(propsPath)
	if err != nil {
		return "/tmp/kafka-logs"
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "log.dirs=") {
			value := strings.TrimPrefix(line, "log.dirs=")
			// Handle potential CSV values, take the first one
			if parts := strings.Split(value, ","); len(parts) > 0 {
				return strings.TrimSpace(parts[0])
			}
			return value
		}
	}
	return "/tmp/kafka-logs"
}

func zeroUUID() []byte {
	return make([]byte, 16)
}


func readTopicIDFromMetadata(topicName string) ([]byte, int16) {
	logDir := parseLogDirectory()
	
	metadataPath := filepath.Join(logDir, "__cluster_metadata-0", "00000000000000000000.log")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return defaultTopicID(), errorUnknownTopic
	}
	topicNameBytes := []byte(topicName)
	idx := bytes.Index(data, topicNameBytes)
	if idx == -1 {
		return defaultTopicID(), errorUnknownTopic
	}

	// UUID is usually 16 bytes following the name in the record
	idStart := idx + len(topicNameBytes)
	if idStart+16 <= len(data) {
		res := make([]byte, 16)
		copy(res, data[idStart:idStart+16])
		return res, errorNone
	}

	return defaultTopicID(), errorUnknownTopic



	// if logDir == "" {
	// 	return defaultTopicID()
	// }

	// metadataPath := filepath.Join(logDir, "__cluster_metadata-0", "00000000000000000000.log")
	// data, err := os.ReadFile(metadataPath)
	// if err != nil {
	// 	return defaultTopicID()
	// }

	// nameBytes := []byte(topicName)
	// if len(nameBytes) == 0 {
	// 	return cloneBytes(emptyTopicID)
	// }

	// // TopicRecord stores compact topic name, then 16-byte topic ID.
	// for i := 1; i+len(nameBytes)+16 <= len(data); i++ {
	// 	if !bytes.Equal(data[i:i+len(nameBytes)], nameBytes) {
	// 		continue
	// 	}

	// 	if int(data[i-1]) != len(nameBytes)+1 {
	// 		continue
	// 	}

	// 	idStart := i + len(nameBytes)
	// 	return cloneBytes(data[idStart : idStart+16])
	// }

	// return defaultTopicID()

}



// func processKafkaRequest (connection net.Conn, bodyBuffer []byte){
// 	if len(bodyBuffer) < 8 {
// 		return
// 	}

//     // first 2 bytes is API Key
//     apiKey := int16(binary.BigEndian.Uint16(bodyBuffer[0:2]))
//     // second 2 bytes are API Version
//     apiVersion := int16(binary.BigEndian.Uint16(bodyBuffer[2:4]))
//     // index 4 to 7 the third 4 bytes are Correlation ID
//     correlationID := binary.BigEndian.Uint32(bodyBuffer[4:8])

//     // Check for valid version first
//     if apiVersion < 0 || apiVersion > 4 {
//         sendError(connection, correlationID, errorApiUnsupportedVersion)
//         return
//     }

//     // Route based on API Key and each case is API versions
//     switch apiKey {
//     case 18: // ApiVersions
//         sendApiVersionResponse(connection, correlationID)
//     case 75: // DescribeTopicPartitions
// 		if len(bodyBuffer) < 10 {
// 			return
// 		}

// 		// 1. Skip Client ID: [Length (2 bytes)] + [String Content]
// 		// Kafka nullable string uses -1 for null.
// 		clientIDLen := int(int16(binary.BigEndian.Uint16(bodyBuffer[8:10])))
// 		currentPos := 10
// 		if clientIDLen > 0 {
// 			currentPos += clientIDLen
// 		}
// 		if currentPos >= len(bodyBuffer) {
// 			return
// 		}

// 		// 2. Skip request header tagged fields (uvarint)
// 		_, n := binary.Uvarint(bodyBuffer[currentPos:])
// 		if n <= 0 {
// 			return
// 		}
// 		currentPos += n
// 		if currentPos >= len(bodyBuffer) {
// 			return
// 		}

// 		// 3. Topics compact array length (uvarint)
// 		_, n = binary.Uvarint(bodyBuffer[currentPos:])
// 		if n <= 0 {
// 			return
// 		}
// 		currentPos += n
// 		if currentPos >= len(bodyBuffer) {
// 			return
// 		}

// 		// 4. Topic object -> name (compact string)
// 		topicName, nameEnd, err := parseTopicNameWithEnd(bodyBuffer[currentPos:])
// 		if err != nil {
// 			return
// 		}
// 		currentPos += nameEnd
// 		if currentPos >= len(bodyBuffer) {
// 			return
// 		}

// 		// 5. Skip topic tagged fields (uvarint)
// 		_, n = binary.Uvarint(bodyBuffer[currentPos:])
// 		if n <= 0 {
// 			return
// 		}

//         processTopicPartitionResponse(connection, correlationID, topicName)
//     default:
//         fmt.Printf("Unsupported API Key: %d\n", apiKey)
//     }

// }

func sendError(connection net.Conn, correlationID uint32, errorCode int16){
	response := make([] byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 6)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

	connection.Write(response)
}



// func sendApiVersionResponse(connection net.Conn, correlationID uint32){

// 	body := [] byte {}
// 	body = append(body, 0, 0)		// error code, 0 means no error
// 	body = append(body, 3)			// 2 entries plush one, bcx sending two api entries			

// 	apiKeyBuffer18 := make([] byte, 6)

// 	binary.BigEndian.PutUint16(apiKeyBuffer18[0:2], 18) // api key
// 	binary.BigEndian.PutUint16(apiKeyBuffer18[2:4], 0)  // min api version
// 	binary.BigEndian.PutUint16(apiKeyBuffer18[4:6], 4)  // max api version

// 	body = append(body, apiKeyBuffer18...)    // api key compact array
// 	body = append(body, 0) 					// tagged fields


// 	apiKeyBuffer75 := make([] byte, 6)

// 	binary.BigEndian.PutUint16(apiKeyBuffer75[0:2], 75) // api key
// 	binary.BigEndian.PutUint16(apiKeyBuffer75[2:4], 0)  // min api version
// 	binary.BigEndian.PutUint16(apiKeyBuffer75[4:6], 4)  // max api version

// 	body = append(body, apiKeyBuffer75...)    // api key compact array
// 	body = append(body, 0) 					// tagged fields

// 	throttleBuffer := make([] byte, 4) 		// creating a throttle time compact array usually is 0
// 	binary.BigEndian.PutUint32(throttleBuffer, 0)
// 	body = append(body, throttleBuffer...)		// adding throttle time compact array data into body
	
// 	body = append(body, 0)					// main tagged fields; there must be a 0 byte after each api key entry, not only one at the end.


// 	// final body packet or final response
// 	// 4 bytes messageSize + 4 bytes correlationID + len(body)
// 	totalSize := 4 + len(body)				// storing total length of body plus correlationID.
// 	response := make([] byte, 4 + totalSize)		// creating response slice containing messageSize plus totalSize

// 	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))   // setting first 4 bytes of response as messageSize
// 	binary.BigEndian.PutUint32(response[4:8], correlationID)		// setting second 4 bytes of response as correlationID
// 	copy(response[8:], body)									// setting from 8th bytes to the end as body of response

// 	connection.Write(response)								// writing/sending response throught the connection
// }


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
		apiVersion := int16(binary.BigEndian.Uint16(bodyBuffer[2:4]))
		correlationID := binary.BigEndian.Uint32(requestBuffer[4:8])

		switch apiKey {
		case 18:
			sendApiVersionResponse(connection, correlationID, apiVersion)
		case 75:
			// Manual skip of header to find Topic Name
			curr := 8
			clientIDLen := int(int16(binary.BigEndian.Uint16(requestBuffer[curr : curr+2])))
			if clientIDLen > 0 { curr += clientIDLen }
			curr += 2 // clientID length field
			curr++    // tag buffer

			// Read Topics Array Length (Compact)
			_, n := binary.Uvarint(requestBuffer[curr:])
			curr += n

			// Read Topic Name (Compact String)
			nameLen, n := binary.Uvarint(requestBuffer[curr:])
			curr += n
			topicName := string(requestBuffer[curr : curr+int(nameLen)-1])

			processTopicPartitionResponse(connection, correlationID, topicName)
		}

	}
}


func sendApiVersionResponse(connection net.Conn, correlationID uint32, apiVersion int16) {
	var b bytes.Buffer

	if apiVersion < 0 || apiVersion > 4 {
        body = append(body, 0, 35) // error code 35: UNSUPPORTED_VERSION
    } else {
        body = append(body, 0, 0)  // error code 0: No error
    }
	
	binary.Write(&b, binary.BigEndian, correlationID)
	binary.Write(&b, binary.BigEndian, errorNone)
	
	b.WriteByte(3) // API Keys count + 1
	// ApiVersions (18)
	binary.Write(&b, binary.BigEndian, int16(18)); binary.Write(&b, binary.BigEndian, int16(0)); binary.Write(&b, binary.BigEndian, int16(4)); b.WriteByte(0)
	// DescribeTopicPartitions (75)
	binary.Write(&b, binary.BigEndian, int16(75)); binary.Write(&b, binary.BigEndian, int16(0)); binary.Write(&b, binary.BigEndian, int16(0)); b.WriteByte(0)

	binary.Write(&b, binary.BigEndian, uint32(0)) // Throttle
	b.WriteByte(0) // Tagged

	final := make([]byte, 4+b.Len())
	binary.BigEndian.PutUint32(final[0:4], uint32(b.Len()))
	copy(final[4:], b.Bytes())
	connection.Write(final)
}

func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicName string) {
	topicID, errCode := readTopicIDFromMetadata(topicName)

	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, correlationID)
	b.WriteByte(0) // Header Tagged Fields

	binary.Write(&b, binary.BigEndian, uint32(0)) // Throttle
	b.WriteByte(2) // Topic Array Length (1 + 1)

	// Topic Object
	binary.Write(&b, binary.BigEndian, errCode)
	b.WriteByte(byte(len(topicName) + 1))
	b.WriteString(topicName)
	b.Write(topicID) // FIX: This is now either the real ID or all zeros
	b.WriteByte(0)   // IsInternal
	
	// Partitions Array
	if errCode != errorNone {
		b.WriteByte(1) // Empty array (0 + 1)
	} else {
		b.WriteByte(2) // 1 partition (1 + 1)
		binary.Write(&b, binary.BigEndian, errorNone)
		binary.Write(&b, binary.BigEndian, uint32(0)) // Index
		binary.Write(&b, binary.BigEndian, uint32(1)) // Leader
		binary.Write(&b, binary.BigEndian, uint32(0)) // Epoch
		b.WriteByte(2); binary.Write(&b, binary.BigEndian, uint32(1)) // Replicas
		b.WriteByte(2); binary.Write(&b, binary.BigEndian, uint32(1)) // ISR
		b.WriteByte(1); b.WriteByte(1); b.WriteByte(1); b.WriteByte(0) // Misc fields
	}

	binary.Write(&b, binary.BigEndian, uint32(0x00000df8)) // Authorized Ops
	b.WriteByte(0)    // Topic Tags
	b.WriteByte(0xff) // Cursor
	b.WriteByte(0)    // Main Tags

	final := make([]byte, 4+b.Len())
	binary.BigEndian.PutUint32(final[0:4], uint32(b.Len()))
	copy(final[4:], b.Bytes())
	connection.Write(final)
}



// func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicName string){
// 	body := []byte {}

// 	throttle := make([] byte, 4) 			// throttle time 4 bytes
// 	binary.BigEndian.PutUint32(throttle, 0)
// 	body = append(body, throttle...)

// 	body = append(body, 2)			// Topic

// 	topicError := make([] byte, 2)			//topic error, error code 3 for UNKNOWN TOPIC OR PARTITION
// 	binary.BigEndian.PutUint16(topicError, 3)
// 	body = append(body, topicError...)

// 	// Topic Name (Compact String: length+1 followed by string)
// 	body = append(body, byte(len(topicName)+1))
// 	body = append(body, []byte(topicName)...)

// 	// Topic ID (16 bytes of zeros for unknown topic)
// 	body = append(body, make([]byte, 16)...)

// 	// Is Internal (1 byte)
// 	body = append(body, 0)
// 	// Partitions Array (Compact: 0 partitions, so length is 0 + 1 = 1)
// 	body = append(body, 1)

// 	// Topic Authorized Operations (4 bytes)
// 	authOps := make([]byte, 4)
// 	binary.BigEndian.PutUint32(authOps, 3576) // Or 0x00000df8 if required
// 	body = append(body, authOps...)

// 	// Tagged Fields for this topic
// 	body = append(body, 0)

// 	// 3. Next Cursor (1 byte) - 0xff indicates null/no cursor
// 	body = append(body, 0xff)

// 	// 4. Main Tagged Fields
// 	body = append(body, 0)

// 	// Final Packet: [Size] + [Correlation ID] + [Body]
// 	totalSize := 4 + len(body)
// 	response := make([]byte, 4+totalSize)
// 	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))
// 	binary.BigEndian.PutUint32(response[4:8], correlationID)
// 	copy(response[8:], body)

// 	connection.Write(response)

// }

// func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicName string) {
// 	body := []byte{}

// 	// 1. Throttle Time (4 bytes)
// 	throttle := make([]byte, 4)
// 	binary.BigEndian.PutUint32(throttle, 0)
// 	body = append(body, throttle...)

// 	// 2. Topics Array (Compact: 1 topic + 1 = 2)
// 	body = append(body, 2)

// 	// // -- Topic Object --
// 	// topicErr := make([]byte, 2)
// 	// binary.BigEndian.PutUint16(topicErr, 3) // UNKNOWN_TOPIC_OR_PARTITION
// 	// body = append(body, topicErr...)

// 	body = append(body, 0, 0) // topic start ojb; error code 0 means success

// 	// Topic Name (Compact String)
// 	body = append(body, byte(len(topicName)+1))
// 	body = append(body, []byte(topicName)...)

// 	// body = append(body, make([]byte, 16)...) // Topic ID (zeros)
// 	// body = append(body, 0)                   // Is Internal
// 	// body = append(body, 1)                   // Partitions Array (0+1=1)

//     // Expected UUID: 71a59a51-8968-4f8b-937e-754e9c2593eb
// 	// Convert the expected UUID string to 16 bytes
//     // uuidString := "71a59a5189684f8b937e754e9c2593eb"
//     // topicID, _ := hex.DecodeString(uuidString) 
// 	body = append(body, readTopicIDFromMetadata(topicName)...)
// 	body = append(body, 0)		// 0 internal bool false
// 	body = append(body, 2)		// 1 partition array length equals to len of 2

// 	body = append(body, 0, 0)	// partition error code 2 bytes

// 	//partition index array 4 bytes
// 	partitionIndex := make([] byte, 4)
// 	binary.BigEndian.PutUint32(partitionIndex, 0)
// 	body = append(body, partitionIndex...)

// 	//leader id 4 bytes
// 	leaderID := make([] byte, 4)
// 	binary.BigEndian.PutUint32(leaderID, 1)  	// assuming your borker id is 1
// 	body = append(body, leaderID...)

// 	//leader epoch 4 bytes
// 	body = append(body, 0,0,0,0)

// 	// Replicas (Compact Array: length 2, node ID 1)
// 	body = append(body, 2, 0, 0, 0, 1)
// 	// ISR Nodes (Compact Array: length 2, node ID 1)
// 	body = append(body, 2, 0, 0, 0, 1)

// 	// Remaining Arrays (Empty Compact Arrays = length 1)
// 	body = append(body, 1) // ELR
// 	body = append(body, 1) // Last Known ELR
// 	body = append(body, 1) // Offline Replicas

// 	body = append(body, 0) // Tagged Fields for Partition
// 	// --- End Partitions Array ---


// 	authOps := make([]byte, 4)
// 	binary.BigEndian.PutUint32(authOps, 3576) // 0x00000df8 (standard for this stage)
// 	body = append(body, authOps...)
// 	body = append(body, 0) // Tagged Fields for Topic

// 	// 3. Cursor & Main Tagged Fields
// 	body = append(body, 0xff) // Next Cursor
// 	body = append(body, 0)    // Main Tagged Fields

// 	// --- FINAL ASSEMBLY (The Critical Part) ---
// 	// totalSize is CorrelationID (4) + header tagged field size 1 byte + len of body
// 	headerTaggedFieldSize := 1
// 	totalSize := 4 + headerTaggedFieldSize + len(body)
// 	response := make([]byte, 4 + totalSize)        // messageSize + totalSize

// 	// Bytes 0-3: Total Message Size
// 	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))
// 	// Bytes 4-7: Correlation ID (Response Header v0)
// 	binary.BigEndian.PutUint32(response[4:8], correlationID)
// 	// Bytes 9+: Body
// 	copy(response[9:], body)

// 	connection.Write(response)
// }



// func parseTopicName(body []byte) (string, error) {
//     //Skip Header: API Key (2), Version (2), CorrelationID (4)
//     offset := 8

//     //Handle ClientID (Nullable String) correctly
//     clientIDLenRaw := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
//     offset += 2
    
//     // In Kafka, -1 (0xFFFF) means Null, 0 means Empty string.
//     // Both take 0 additional bytes.
//     if clientIDLenRaw > 0 {
//         offset += int(clientIDLenRaw)
//     }

//     //Skip Request Tagged Buffer (1 byte)
//     offset += 1

//     // Read Topics Array Length (Compact Array)
//     // The tester sends 1 topic, so this byte is 2 (N+1)
//     offset += 1

//     // Read Topic Name (Compact String)
//     // First byte is length + 1. If length is > 128, this is a varint, 
//     // but for the tester, 1 byte is usually enough.
//     nameLen := int(body[offset]) - 1
//     offset += 1
    
//     if offset+nameLen > len(body) {
//         return "", fmt.Errorf("buffer overflow")
//     }

//     topicName := string(body[offset : offset+nameLen])
//     return topicName, nil
// }


// // parseTopicNameWithEnd returns the topic name and the index where it ends in the buffer.
// func parseTopicNameWithEnd(buffer []byte) (string, int, error) {
//     // 1. Skip the topics array length (1 byte for a compact array of 1 topic)
//     // 2. Read the Compact String length (VarInt)
//     length, n := binary.Uvarint(buffer) 
//     if n <= 0 {
//         return "", 0, fmt.Errorf("invalid varint")
//     }

//     // Kafka Compact Strings use length + 1
//     actualLength := int(length) - 1
//     if actualLength < 0 {
//         return "", n, nil // Null string case
//     }

// 	if n+actualLength > len(buffer) {
// 		return "", 0, fmt.Errorf("buffer overflow while parsing topic name")
// 	}
//     topicName := string(buffer[n : n + actualLength])

//     return topicName, n+actualLength, nil
// }


func main() {

	// l, _ := net.Listen("tcp", "0.0.0.0:9092")
	// for{
	// 	connection, _ := l.Accept()
	// 	go handleKafkaRequest(connection)
	// }


	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	// defer l.Close()

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClientRequest(connection)

	}
}

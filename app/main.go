package main

import (
	"fmt"
	"net"
	"os"
	"encoding/binary"
	"io"
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
	errorApiUnsupportedVersion int16 = 35
)

func processKafkaRequest (connection net.Conn, bodyBuffer []byte){

    // first 2 bytes is API Key
    apiKey := int16(binary.BigEndian.Uint16(bodyBuffer[0:2]))
    // second 2 bytes are API Version
    apiVersion := int16(binary.BigEndian.Uint16(bodyBuffer[2:4]))
    // index 4 to 7 the third 4 bytes are Correlation ID
    correlationID := binary.BigEndian.Uint32(bodyBuffer[4:8])

    // Check for valid version first
    if apiVersion < 0 || apiVersion > 4 {
        sendError(connection, correlationID, errorApiUnsupportedVersion)
        return
    }

    // Route based on API Key and each case is API versions
    switch apiKey {
    case 18: // ApiVersions
        sendApiVersionResponse(connection, correlationID)
    case 75: // DescribeTopicPartitions
		topicName, err := parseTopicName(bodyBuffer)
		if err != nil{
			fmt.Println("Error parsing topic name", err)
			return
		}
        // For now, you can hardcode a topic name like "unknown_topic" 
        // until you implement the code to parse it from the request body.
        processTopicPartitionResponse(connection, correlationID, topicName)
    default:
        fmt.Printf("Unsupported API Key: %d\n", apiKey)
    }

}

func sendError(connection net.Conn, correlationID uint32, errorCode int16){
	response := make([] byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 6)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

	connection.Write(response)
}



func sendApiVersionResponse(connection net.Conn, correlationID uint32){

	body := [] byte {}
	body = append(body, 0, 0)		// error code, 0 means no error
	body = append(body, 3)			// 2 entries plush one, bcx sending two api entries			

	apiKeyBuffer18 := make([] byte, 6)

	binary.BigEndian.PutUint16(apiKeyBuffer18[0:2], 18) // api key
	binary.BigEndian.PutUint16(apiKeyBuffer18[2:4], 0)  // min api version
	binary.BigEndian.PutUint16(apiKeyBuffer18[4:6], 4)  // max api version

	body = append(body, apiKeyBuffer18...)    // api key compact array
	body = append(body, 0) 					// tagged fields


	apiKeyBuffer75 := make([] byte, 6)

	binary.BigEndian.PutUint16(apiKeyBuffer75[0:2], 75) // api key
	binary.BigEndian.PutUint16(apiKeyBuffer75[2:4], 0)  // min api version
	binary.BigEndian.PutUint16(apiKeyBuffer75[4:6], 4)  // max api version

	body = append(body, apiKeyBuffer75...)    // api key compact array
	body = append(body, 0) 					// tagged fields

	throttleBuffer := make([] byte, 4) 		// creating a throttle time compact array usually is 0
	binary.BigEndian.PutUint32(throttleBuffer, 0)
	body = append(body, throttleBuffer...)		// adding throttle time compact array data into body
	
	body = append(body, 0)					// main tagged fields; there must be a 0 byte after each api key entry, not only one at the end.


	// final body packet or final response
	// 4 bytes messageSize + 4 bytes correlationID + len(body)
	totalSize := 4 + len(body)				// storing total length of body plus correlationID.
	response := make([] byte, 4 + totalSize)		// creating response slice containing messageSize plus totalSize

	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))   // setting first 4 bytes of response as messageSize
	binary.BigEndian.PutUint32(response[4:8], correlationID)		// setting second 4 bytes of response as correlationID
	copy(response[8:], body)									// setting from 8th bytes to the end as body of response

	connection.Write(response)								// writing/sending response throught the connection
}


func handleClientRequest(connection net.Conn){
	defer connection.Close()  // close connectiong if when loop breaks

	for{
		sizeBuffer := make([]byte, 4)
		_, err := io.ReadFull(connection, sizeBuffer)
		if err != nil{
			return 		// exit the loop and close connection
		}
		
		messageSize := binary.BigEndian.Uint32(sizeBuffer)
		requestBuffer := make([]byte, messageSize)
		if _, err := io.ReadFull(connection, requestBuffer); err != nil{
			fmt.Println("Error reading request body", err)
			return
		}

		processKafkaRequest(connection, requestBuffer)
	}
}


func processTopicPartitionResponse(connection net.Conn, correlationID uint32, topicName string){
	body := []byte {}

	throttle := make([] byte, 4) 			// throttle time 4 bytes
	binary.BigEndian.PutUint32(throttle, 0)
	body = append(body, throttle...)

	body = append(body, 2)			// Topic

	topicError := make([] byte, 2)			//topic error, error code 3 for UNKNOWN TOPIC OR PARTITION
	binary.BigEndian.PutUint16(topicError, 3)
	body = append(body, topicError...)

	// Topic Name (Compact String: length+1 followed by string)
	body = append(body, byte(len(topicName)+1))
	body = append(body, []byte(topicName)...)

	// Topic ID (16 bytes of zeros for unknown topic)
	body = append(body, make([]byte, 16)...)

	// Is Internal (1 byte)
	body = append(body, 0)

	// Partitions Array (Compact: 0 partitions, so length is 0 + 1 = 1)
	body = append(body, 1)

	// Topic Authorized Operations (4 bytes)
	authOps := make([]byte, 4)
	binary.BigEndian.PutUint32(authOps, 0) // Or 0x00000df8 if required
	body = append(body, authOps...)

	// Tagged Fields for this topic
	body = append(body, 0)

	// 3. Next Cursor (1 byte) - 0xff indicates null/no cursor
	body = append(body, 0xff)

	// 4. Main Tagged Fields
	body = append(body, 0)

	// Final Packet: [Size] + [Correlation ID] + [Body]
	totalSize := 4 + len(body)
	response := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	copy(response[8:], body)

	connection.Write(response)

}


func parseTopicName(body []byte) (string, error) {
    //Skip Header: API Key (2), Version (2), CorrelationID (4)
    offset := 8

    //Handle ClientID (Nullable String) correctly
    clientIDLenRaw := int16(binary.BigEndian.Uint16(body[offset : offset+2]))
    offset += 2
    
    // In Kafka, -1 (0xFFFF) means Null, 0 means Empty string.
    // Both take 0 additional bytes.
    if clientIDLenRaw > 0 {
        offset += int(clientIDLenRaw)
    }

    //Skip Request Tagged Buffer (1 byte)
    offset += 1

    // Read Topics Array Length (Compact Array)
    // The tester sends 1 topic, so this byte is 2 (N+1)
    offset += 1

    // Read Topic Name (Compact String)
    // First byte is length + 1. If length is > 128, this is a varint, 
    // but for the tester, 1 byte is usually enough.
    nameLen := int(body[offset]) - 1
    offset += 1
    
    if offset+nameLen > len(body) {
        return "", fmt.Errorf("buffer overflow")
    }

    topicName := string(body[offset : offset+nameLen])
    return topicName, nil
}




func main() {

	// l, _ := net.Listen("tcp", "0.0.0.0:9092")
	// for{
	// 	connection, _ := l.Accept()
	// 	go handleKafkaRequest(connection)
	// }


	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		return
	}
	defer l.Close()

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleClientRequest(connection)

	}
}

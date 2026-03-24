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

	
	apiVersion := int16(binary.BigEndian.Uint16(bodyBuffer[2:4]))
	correlationID := binary.BigEndian.Uint32(bodyBuffer[4:8])

	if apiVersion < 0 || apiVersion > 4 {
		sendError(connection, correlationID, errorApiUnsupportedVersion)
		return
	}

	sendApiVersionResponse(connection, correlationID)

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
	body = append(body, 0) 			75		// tagged fields


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

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
	body = append(body, 2)			

	apiKeyBuffer := make([] byte, 6)

	binary.BigEndian.PutUint16(apiKeyBuffer[0:2], 18) // api key
	binary.BigEndian.PutUint16(apiKeyBuffer[2:4], 0)  // min api version
	binary.BigEndian.PutUint16(apiKeyBuffer[4:6], 4)  // max api version

	body = append(body, apiKeyBuffer...)    // api key compact array
	body = append(body, 0) 					// tagged fields

	throttleBuffer := make([] byte, 4) 		// creating a throttle time compact array usually is 0
	binary.BigEndian.PutUint32(throttleBuffer, 0)
	body = append(body, throttleBuffer...)		// adding throttle time compact array data into body
	
	body = append(body, 0)					// main tagged fields

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

// func sendResponse(connection net.Conn, correlationID uint32, errorCode uint16){
// 	res := make([] byte, 8) 			// 4 bytes messageSize plus 4 bytes correlationID
// 	binary.BigEndian.PutUint32(res[0:4], 4)
// 	binary.BigEndian.PutUint32(res[4:8], correlationID)
// 	binary.BigEndian.PutUint16(res[8:10], uint16(errorCode))

// 	connection.Write(res)
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

		// err = sendResponse(connection)
		// if err != nil{
		// 	fmt.Println("Error sending Kafka response", err.Error)
		// }
		// connection.Close()
	}
}





// package main

// import (
// 	"fmt"
// 	"net"
// 	"os"
// 	"encoding/binary"
// 	"io"
// )

// var _ = net.Listen
// var _ = os.Exit



// const(
// 	errorNone int16 = 0
// 	errorApiUnsupportedVersion int16 = 35
// )

// func handleKafkaRequest (connection net.Conn){
// 	defer connection.Close()

// 	sizeBuffer := make([]byte, 4)
// 	if _, err := io.ReadFull(connection, sizeBuffer); err != nil{
// 		return
// 	}

// 	messageSize := binary.BigEndian.Uint32(sizeBuffer)

// 	bodyBuffer := make([] byte, messageSize)
// 	if _, err := io.ReadFull(connection, bodyBuffer); err != nil{
// 		return
// 	}
// 	apiKey := int16(binary.BigEndian.Uint16(bodyBuffer[0:2]))
// 	apiVersion := int16(binary.BigEndian.Uint16(bodyBuffer[2:4]))
// 	correlationID := binary.BigEndian.Uint32(bodyBuffer[4:8])
// 	fmt.Printf("Received Correlation ID: %d, apiKey: %d\n", correlationID, apiKey)

// 	if apiVersion < 0 || apiVersion > 4 {
// 		sendError(connection, correlationID, errorApiUnsupportedVersion)
// 		return
// 	}

// 	sendApiVersionResponse(connection, correlationID)

// }

// func sendError(connection net.Conn, correlationID uint32, errorCode int16){
// 	response := make([] byte, 10)
// 	binary.BigEndian.PutUint32(response[0:4], 6)
// 	binary.BigEndian.PutUint32(response[4:8], correlationID)
// 	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

// 	connection.Write(response)
// }



// func sendApiVersionResponse(connection net.Conn, correlationID uint32){

// 	body := [] byte {}
// 	body = append(body, 0, 0)
// 	body = append(body, 2)

// 	apiKeyBuffer := make([] byte, 6)

// 	binary.BigEndian.PutUint16(apiKeyBuffer[0:2], 18) // api key
// 	binary.BigEndian.PutUint16(apiKeyBuffer[2:4], 0)  // min api version
// 	binary.BigEndian.PutUint16(apiKeyBuffer[4:6], 4)  // max api version

// 	body = append(body, apiKeyBuffer...)    // api key compact array
// 	body = append(body, 0) 					// tagged fields

// 	throttleBuffer := make([] byte, 4) 		// creating a throttle time compact array usually is 0
// 	throttleBuffer = append(throttleBuffer, 0)

// 	body = append(body, throttleBuffer...)		// adding throttle time compact array data into body
// 	body = append(body, 0)					// main tagged fields

// 	// final body packet 
// 	// 4 bytes messageSize + 4 bytes correlationID + len(body)

// 	totalSize := 4 + len(body)				// storing total length of body plus correlationID.

// 	response := make([] byte, 4 + totalSize)		// creating response slice containing messageSize plus totalSize

// 	binary.BigEndian.PutUint32(response[0:4], uint32(totalSize))   // setting first 4 bytes of response as messageSize
// 	binary.BigEndian.PutUint32(response[4:8], correlationID)		// setting second 4 bytes of response as correlationID
// 	copy(response[8:], body)									// setting from 8th bytes to the end as body of response

// 	connection.Write(response)								// writing/sending response throught the connection
// }


// func handleClientRequest(connection net.Conn){
// 	defer connection.Close()  // close connectiong if when loop breaks

// 	for{
// 		sizeBuffer := make([]byte, 4)
// 		_, err := io.ReadFull(connection, sizeBuffer)
// 		if err != nil{
// 			if err != io.EOF{
// 				fmt.Println("Client disconnected or error", err)
// 			}
// 			return 		// exit the loop and close connection
// 		}
		
// 		messageSize := binary.BigEndian.Uint32(sizeBuffer)
// 		requestBuffer := make([]byte, messageSize)
// 		if _, err := io.ReadFull(connection, requestBuffer); err != nil{
// 			fmt.Println("Error reading request body", err)
// 			return
// 		}

// 		correlationID := binary.BigEndian.Uint32(requestBuffer[4:8])
// 		// errorCode := binary.BigEndian.Uint16(requestBuffer[8:10])
// 		fmt.Printf("Processing request with correlationID %d\n", correlationID)

// 		handleKafkaRequest(connection)
// 	}
// }



// func main() {	
// 	l, err := net.Listen("tcp", "0.0.0.0:9092")
// 	if err != nil {
// 		fmt.Println("Failed to bind to port 9092")
// 		return
// 	}
// 	defer l.Close()

// 	for {
// 		connection, err := l.Accept()
// 		if err != nil {
// 			fmt.Println("Error accepting connection: ", err.Error())
// 			continue
// 		}

// 		go handleClientRequest(connection)

// 	}
// }

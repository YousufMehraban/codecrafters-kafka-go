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

func handleKafkaRequest (connection net.Conn){
	defer connection.Close()

	sizeBuffer := make([]byte, 4)
	if _, err := io.ReadFull(connection, sizeBuffer); err != nil{
		return
	}

	messageSize := binary.BigEndian.Uint32(sizeBuffer)

	bodyBuffer := make([] byte, messageSize)
	if _, err := io.ReadFull(connection, bodyBuffer); err != nil{
		return
	}
	apiKey := binary.BigEndian.Uint16(bodyBuffer[0:2])
	apiVersion := binary.BigEndian.Uint16(bodyBuffer[2:4])
	correlationID := binary.BigEndian.Uint32(bodyBuffer[4:8])
	fmt.Printf("Received Correlation ID: %d, apiKey: %d\n", correlationID, )

	if apiVersion < 0 || apiVersion > 4 {
		sendError(connection, correlationID, errorApiUnsupportedVersion)
		return
	}

}

func sendError(connection net.Conn, correlationID uint32, errorCode int16){
	response := make([] byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 6)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

	connection.Write(response)
}








func main() {

	l, _ := net.Listen("tcp", "0.0.0.0:9092")
	for{
		connection, _ := l.Accept()
		go handleKafkaRequest(connection)
	}







	// // You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	// // TODO: Uncomment the code below to pass the first stage
	
	// l, err := net.Listen("tcp", "0.0.0.0:9092")
	// if err != nil {
	// 	fmt.Println("Failed to bind to port 9092")
	// 	os.Exit(1)
	// }

	// for {
	// 	connection, err := l.Accept()
	// 	if err != nil {
	// 		fmt.Println("Error accepting connection: ", err.Error())
	// 		continue
	// 	}

	// 	err = sendResponse(connection)
	// 	if err != nil{
	// 		fmt.Println("Error sending Kafka response", err.Error)
	// 	}
	// 	connection.Close()
	// }
}

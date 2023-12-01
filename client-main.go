package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	serverName = flag.String("b", "", "bootstrap server name")
	startDelay := flag.Int("d", 0, "the delay before starting up")
	storeName = flag.String("o", "", "object store filename")
	RequestValue := flag.Int("t", 0, "the value that should be stored")

	flag.Parse()
	var err error
	hostname, err = os.Hostname()
	checkIfError(err, "Error getting hostname:")

	duration := time.Duration(*startDelay) * time.Second
	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)

	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")

	go listenClient(listener)

	var myRequest Request
	time.Sleep(duration)
	switch *RequestValue {
	case 3:
		myRequest = Request{
			ReqId:    1,
			ObjId:    20,
			ClientId: 1,
			Op:       STORE,
			Res:      UNDEFINED,
		}
		break
	case 4:
		myRequest = Request{
			ReqId:    1,
			ObjId:    100,
			ClientId: 1,
			Op:       RETRIEVE,
			Res:      UNDEFINED,
		}
		break
	case 5:
		myRequest = Request{
			ReqId:    1,
			ObjId:    125,
			ClientId: 1,
			Op:       RETRIEVE,
			Res:      UNDEFINED,
		}
		break
	default:
		fmt.Fprintf(os.Stderr, "ERROR")
	}

	sendRequest(*serverName, myRequest)
	fmt.Fprintf(os.Stderr, "Sent request\n")

	select {}
}

// listens for the data
func listenClient(listener net.Listener) {

	for {
		connection, err := listener.Accept()
		checkIfError(err, "Error accepting: ")

		// fmt.Fprintln(os.Stderr,"client connected")
		remoteAddr := connection.RemoteAddr().String()
		ip, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error splitting host and port:", err)
			return
		}

		hostnames, err := net.LookupAddr(ip)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error performing reverse DNS lookup:", err)
			return
		}

		peerName := strings.Split(hostnames[0], ".")
		go receiveValueClient(connection, peerName[0])
	}
}

func recvfrom_bootstrap(req Request) {
	if req.Res == SUCCESS {
		if req.Op == STORE {
			fmt.Fprintln(os.Stderr, "STORED: ", req.ObjId)
		}
		if req.Op == RETRIEVE {
			fmt.Fprintln(os.Stderr, "RETRIEVED: ", req.ObjId)
		}
	}

	if req.Res == FAILURE {
		if req.Op == RETRIEVE {
			fmt.Fprintln(os.Stderr, "NOT FOUND: ", req.ObjId)

		}
	}
}

func receiveValueClient(connection net.Conn, peerName string) {
	defer connection.Close()

	//fmt.Fprintln(os.Stderr, "Receiving a value")
	// Read prOposal from the connection
	buffer := make([]byte, 1024)
	readLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error reading:", err.Error())
		return
	}
	bufferUpdated := buffer[:readLen]
	var data map[string]int
	err = json.Unmarshal(bufferUpdated, &data)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error unmarshalling JSON:", err.Error())
		return
	}

	if _, ok := data["ReqId"]; ok {
		currentRequest = Request{
			ReqId:    int(data["ReqId"]),
			ClientId: int(data["ClientId"]),
			ObjId:    int(data["ObjId"]),
			Res:      ResponseType(data["Res"]),
			Op:       OperationType(data["Op"]),
		}

		recvfrom_bootstrap(currentRequest)

	}
}

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	serverName = flag.String("b", "", "bootstrap server name")
	start_delay := flag.Int("d", 0, "the delay before starting up")
	storeName = flag.String("o", "", "object store filename")

	flag.Parse()
	var err error
	hostname, err = os.Hostname()
	checkIfError(err, "Error getting hostname:")

	duration := time.Duration(*start_delay) * time.Second
	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)

	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")

	go listenPeer(listener)

	time.Sleep(duration)

	OpenObjectFile(*storeName)
	join()

	select {}
}

// listens for the data
func listenPeer(listener net.Listener) {

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
		go receiveValuePeer(connection, peerName[0])
	}
}

func receiveValuePeer(connection net.Conn, peerName string) {
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

	if _, ok := data["Id"]; ok {
		//fmt.Fprintln(os.Stderr, "Receiving PeerData")

		currentData = PeerData{
			Id:          int(data["Id"]),
			Predecessor: int(data["Predecessor"]),
			Successor:   int(data["Successor"]),
		}

		update_pd(currentData)

	}

	if _, ok := data["ReqId"]; ok {
		currentRequest = Request{
			ReqId:    int(data["ReqId"]),
			ClientId: int(data["ClientId"]),
			ObjId:    int(data["ObjId"]),
			Res:      ResponseType(data["Res"]),
			Op:       OperationType(data["Op"]),
		}

		if currentRequest.ObjId > myData.Id {
			sendRequest("n"+strconv.Itoa(myData.Successor), currentRequest)
		} else {
			store_or_retrieve_object(currentRequest)
		}

	}
}

// sends joins to the bootstrap
func join() {
	i, err := strconv.Atoi(strings.Replace(hostname, "n", "", 1))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return
	}
	fmt.Fprintln(os.Stderr, "Peer joining")
	myData = PeerData{
		Id:          i,
		Predecessor: -1,
		Successor:   128,
	}

	sendPeerData(*serverName, myData)
	fmt.Fprintf(os.Stderr, "Peer %d joined\n", i)

}

// Opens the object file and sets objectsMap based on it
func OpenObjectFile(filename string) {
	file, err := os.Open(filename)
	checkIfError(err, "Error Opening file:")

	defer file.Close()

	objectsMap = make(map[string][]int)

	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	// Iterate over each line in the file
	for scanner.Scan() {
		line := scanner.Text()

		// Check for errors during scanning
		err := scanner.Err()
		checkIfError(err, "Error reading file:")

		parts := strings.Split(line, "::")

		// Create a map with the key-value pair
		num, err := strconv.Atoi(parts[1])
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		objectsMap[parts[0]] = append(objectsMap[parts[0]], num)

	}
	fmt.Fprintln(os.Stderr, "objectsMap written: ", objectsMap)

}

// sets myData to received PeerData
func update_pd(pd PeerData) {
	if pd.Id == myData.Id {
		myData = PeerData{
			Id:          pd.Id,
			Predecessor: pd.Predecessor,
			Successor:   pd.Successor,
		}
		fmt.Fprintln(os.Stderr, "Joined the server, updated PeerData:", myData)
	}
	if pd.Predecessor == myData.Id {
		myData.Successor = pd.Id
		fmt.Fprintln(os.Stderr, "Updated my Successor: ", myData)
	}
	if pd.Successor == myData.Id {
		myData.Predecessor = pd.Id
		fmt.Fprintln(os.Stderr, "Updated my Predecessor: ", myData)
	}
}

// receieves a Request and based on the Operation,
// sends it to store_object or retrieve_object
func store_or_retrieve_object(req Request) {
	if req.Op == STORE {
		store_object(req)
	}
	if req.Op == RETRIEVE {
		retrieve_object(req)
	}
}

// uses the receieved Requests ClientId and ObjId to set in the objectsMap
// then overwrites the objects file with the new objectsMap
// then sends the received Request but with a OBJ_STORED message to the server
func store_object(r Request) {
	objectsMap[strconv.Itoa(r.ClientId)] = append(objectsMap[strconv.Itoa(r.ClientId)], r.ObjId)
	var d1 []byte

	for key, value := range objectsMap {
		for _ = range value {
			entry := fmt.Sprintf("%s::%s\n", key, value)
			d1 = append(d1, []byte(entry)...)
		}
		// Concatenate key and value

		// Convert the entry to bytes and append to the byte slice
	}

	err := os.WriteFile(*storeName, d1, 0644)
	checkIfError(err, "writing to file")
	var req = Request{
		ReqId:    r.ReqId,
		Op:       r.Op,
		ObjId:    r.ObjId,
		ClientId: r.ClientId,
		Res:      OBJ_STORED,
	}
	fmt.Fprintln(os.Stderr, "Successfully stored the object, updated objectsMap: ", objectsMap)
	sendRequest(*serverName, req)

}

// recieves a Request and attempts to retrieve object from the objectMap
// if successful it will send the same Request with the SUCCESS Response
// else will send the same Request with the FAILURE Response
func retrieve_object(r Request) {
	var req Request
	var found bool = false

outerLoop:
	for key, value := range objectsMap {
		if key == strconv.Itoa(r.ClientId) {
			for i := range value {
				if r.ObjId == objectsMap[key][i] {
					// if value is found, it will be a success
					req = Request{
						ReqId:    r.ReqId,
						Op:       r.Op,
						ObjId:    r.ObjId,
						ClientId: r.ClientId,
						Res:      SUCCESS,
					}
					fmt.Fprintln(os.Stderr, "Successfully retrieved the object")
					sendRequest(*serverName, req)
					found = true
					break outerLoop // This will break out of the outer loop
				}
			}
		}
	}
	if found == false {
		req = Request{
			ReqId:    r.ReqId,
			Op:       r.Op,
			ObjId:    r.ObjId,
			ClientId: r.ClientId,
			Res:      FAILURE,
		}
		fmt.Fprintln(os.Stderr, "Failed to retrive the object")
		sendRequest(*serverName, req)
	}
}

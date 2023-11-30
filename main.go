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

type operationType int64

const (
	STORE    operationType = iota
	RETRIEVE operationType = iota
	JOIN     operationType = iota
)

type responseType int64

const (
	SUCCESS    responseType = iota
	FAILURE    responseType = iota
	UNDEFINED  responseType = iota
	OBJ_STORED responseType = iota
)

type peerData struct {
	id          int
	predecessor int
	successor   int
}

type request struct {
	reqId    int
	op       operationType
	objId    int
	clientId int
	res      responseType
}

var (
	isBootstrap    bool
	isPeer         bool
	isClient       bool
	peer_list      []int
	store_name     *string
	server_name    *string
	hostname       string
	objectsMap     map[string]string
	myData         peerData
	myRequest      request
	PORT           = "4950"
	currentData    peerData
	currentRequest request
)

func main() {
	server_name = flag.String("b", "", "bootstrap server name")
	start_delay := flag.Int("d", 0, "the delay before starting up")
	store_name = flag.String("o", "", "object store filename")
	request_value := flag.Int("t", 0, "the value that should be stored")

	flag.Parse()
	var err error
	hostname, err = os.Hostname()
	checkIfError(err, "Error getting hostname:")

	currentHostRole(hostname)

	duration := time.Duration(*start_delay) * time.Second
	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)

	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")

	go listen(listener)

	time.Sleep(duration)

	if isClient {
		myRequest := request{
			reqId:    0,
			objId:    *request_value,
			clientId: 1,
			op:       -1,
			res:      UNDEFINED,
		}
		sendRequest(*server_name, myRequest)
	}

	if isPeer {
		openObjectFile(*store_name)
		join()
	}

	select {}
}

// sends joins to the bootstrap
func join() {
	i, err := strconv.Atoi(hostname)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	myData = peerData{
		id:          i,
		predecessor: -1,
		successor:   128,
	}

	sendPeerData(*server_name, myData)
	fmt.Fprintf(os.Stderr, "Peer %d joined\n", i)

}

func checkIfError(e error, message string) {
	if e != nil {
		fmt.Fprintln(os.Stderr, message)
		//panic(e)
	}
}

// listens for the data
func listen(listener net.Listener) {
	defer listener.Close()
	// fmt.Println(os.Stderr, "Listening on ")
	// fmt.Println(os.Stderr, "Waiting for client...")
	for {
		connection, err := listener.Accept()
		checkIfError(err, "Error accepting: ")

		// fmt.Println("client connected")
		remoteAddr := connection.RemoteAddr().String()
		ip, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			fmt.Println("Error splitting host and port:", err)
			return
		}

		hostnames, err := net.LookupAddr(ip)
		if err != nil {
			fmt.Println("Error performing reverse DNS lookup:", err)
			return
		}

		peerName := strings.Split(hostnames[0], ".")
		go receiveValue(connection, peerName[0])

	}
}

func receiveValue(connection net.Conn, peerName string) {
	defer connection.Close()

	// Read proposal from the connection
	buffer := make([]byte, 1024)
	_, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	var data interface{}

	err = json.Unmarshal(buffer, &data)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err.Error())
		return
	}

	switch v := data.(type) {
	case peerData:
		currentData = peerData{
			id:          v.id,
			predecessor: v.predecessor,
			successor:   v.successor,
		}

		if isPeer {
			update_pd(v)
		}

		if isBootstrap {
			add_peer(strconv.Itoa(v.id))
		}
		break
	case request:
		currentRequest = request{
			reqId:    v.reqId,
			clientId: v.clientId,
			objId:    v.objId,
			res:      v.res,
			op:       v.op,
		}

		if isPeer {
			if currentRequest.objId >= myData.id {
				sendRequest(strconv.Itoa(currentData.successor), currentRequest)
			} else {
				store_or_retrieve_object(currentRequest)
			}
		}

		if isBootstrap {
			// received from client if undefined
			if currentRequest.res == UNDEFINED {
				sendRequest(strconv.Itoa(peer_list[0]), currentRequest)
			} else {
				if currentRequest.res == OBJ_STORED {
					currentRequest.res = SUCCESS
				}
				sendRequest(strconv.Itoa(currentRequest.clientId), currentRequest)
			}
		}

		if isClient {
			recvfrom_bootstrap(currentRequest)
		}
		break
	default:
		fmt.Fprintf(os.Stderr, "ERROR: received a non peerData or request value")
		break
	}

}

func recvfrom_bootstrap(req request) {
	if req.res == SUCCESS {
		if req.op == STORE {
			fmt.Println("STORED: ", req.objId)
		}
		if req.op == RETRIEVE {
			fmt.Println("RETRIEVED: ", req.objId)
		}
	}

	if req.res == FAILURE {
		if req.op == RETRIEVE {
			fmt.Println("NOT FOUND: ", req.objId)

		}
	}
}

// takes in a string for peer_id and and a request and
// sends the request to peer_id (can be client, server, or peer)
func sendRequest(peer_id string, req request) {
	var SERVER_TYPE = "tcp"

	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer_id+":"+PORT)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return
	}
	_, err = connection.Write(jsonData)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	defer connection.Close()
}

// takes in a string for peer_id and and a peerData and
// sends the peerData to peer_id (can be client, server, or peer)
func sendPeerData(peer_id string, pd peerData) {
	var SERVER_TYPE = "tcp"

	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer_id+":"+PORT)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}

	jsonData, err := json.Marshal(pd)
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return
	}
	_, err = connection.Write(jsonData)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	defer connection.Close()
}

// based on the hostname it assigns the current hosts role
func currentHostRole(line string) {
	switch {
	case strings.Contains(line, "client"):
		isClient = true

	case strings.Contains(line, "bootstrap"):
		isBootstrap = true
	default:
		isPeer = true
	}
}

// opens the object file and sets objectsMap based on it
func openObjectFile(filename string) {
	file, err := os.Open(filename)
	checkIfError(err, "Error opening file:")

	defer file.Close()

	objectsMap = make(map[string]string)

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
		objectsMap[parts[0]] = parts[1]

	}
	fmt.Fprintf(os.Stderr, "objectsMap written\n")

}

// sets myData to received peerData
func update_pd(pd peerData) {
	if pd.id == myData.id {
		myData = peerData{
			id:          pd.id,
			predecessor: pd.predecessor,
			successor:   pd.successor,
		}
		fmt.Println("Joined the server, updated peerData:", myData)
	}
	if pd.predecessor == myData.id {
		myData.successor = pd.id
		fmt.Println("Updated my successor: ", myData)
	}
	if pd.successor == myData.id {
		myData.predecessor = pd.id
		fmt.Println("Updated my predecessor: ", myData)
	}
}

// receieves a request and based on the operation,
// sends it to store_object or retrieve_object
func store_or_retrieve_object(req request) {
	if req.op == STORE {
		store_object(req)
	}
	if req.op == RETRIEVE {
		retrieve_object(req)
	}
}

// recieves a request and attempts to retrieve object from the objectMap
// if successful it will send the same request with the SUCCESS response
// else will send the same request with the FAILURE response
func retrieve_object(r request) {
	var req request

	for key, value := range objectsMap {
		if strconv.Itoa(r.objId) == value && key == strconv.Itoa(r.clientId) {
			// if value is found, it will be a success
			req = request{
				reqId:    r.reqId,
				op:       r.op,
				objId:    r.objId,
				clientId: r.clientId,
				res:      SUCCESS,
			}
			fmt.Println("Successfully retrieved the object")
			sendRequest(*server_name, req)
			break

		}
	}

	req = request{
		reqId:    r.reqId,
		op:       r.op,
		objId:    r.objId,
		clientId: r.clientId,
		res:      FAILURE,
	}
	fmt.Println("Failed to retrive the object")
	sendRequest(*server_name, req)
}

// uses the receieved requests clientId and objId to set in the objectsMap
// then overwrites the objects file with the new objectsMap
// then sends the received request but with a OBJ_STORED message to the server
func store_object(r request) {
	objectsMap[strconv.Itoa(r.clientId)] = strconv.Itoa(r.objId)
	var d1 []byte

	for key, value := range objectsMap {
		// Concatenate key and value
		entry := fmt.Sprintf("%s::%s\n", key, value)

		// Convert the entry to bytes and append to the byte slice
		d1 = append(d1, []byte(entry)...)
	}

	err := os.WriteFile(*store_name, d1, 0644)
	checkIfError(err, "writing to file")
	var req = request{
		reqId:    r.reqId,
		op:       r.op,
		objId:    r.objId,
		clientId: r.clientId,
		res:      OBJ_STORED,
	}
	fmt.Println("Successfully stored the object, updated objectsMap: ", objectsMap)
	sendRequest(*server_name, req)

}
func contains(list []int, value int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func add_peer(peer_id string) {
	pid, err := strconv.Atoi(peer_id)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	peer_list = append(peer_list, pid)
	fmt.Println("Peer list: ", peer_list)
	calculate_and_inform(peer_id)

}

func calculate_and_inform(peer_id string) {
	var pre int
	pre = 0
	pid, err := strconv.Atoi(peer_id)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	for i := 0; i < len(peer_list); i++ {
		if pid > peer_list[i] && peer_list[i] > pre {
			pre = peer_list[i]
		}
	}
	var suc int
	suc = 128
	for i := 0; i < len(peer_list); i++ {
		if pid < peer_list[i] && peer_list[i] < suc {
			suc = peer_list[i]
		}
	}
	var pd = peerData{
		id:          pid,
		predecessor: pre,
		successor:   suc,
	}
	sendPeerData(peer_id, pd)
	if pre > 0 {
		sendPeerData(strconv.Itoa(myData.predecessor), pd)
	}
	if suc < 128 {
		sendPeerData(strconv.Itoa(myData.successor), pd)
	}
}

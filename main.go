package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"encoding/json"
)

type operationType int64

const (
	STORE operationType = iota
	RETRIEVE operationType = iota
	JOIN operationType = iota
)

type responseType int64
const (
	SUCCESS responseType = iota
	FAILURE responseType = iota
	UNDEFINED responseType = iota
)
type peerData struct {
	id int,
	predecessor int,
	successor int
}

type request struct {
	reqId int,
	op operationType,
	objId int,
	clientId int,
	res responseType
}

var (
	isBootstrap	bool
	isPeer	bool
	isClient bool
	peer_list []int
	store_name string
	server_name string
	hostname string
	objectsMap map[int]int
	myData peerData
	clientname string
)



func checkIfError(e error, message string) {
	if e != nil {
		fmt.Fprintln(os.Stderr, message)
		//panic(e)
	}
}

func contains(list []int, value int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func add_peer(peer_id int) {
	peer_list = append(peer_list, peer_id)
	calculate_and_inform(peer_id);

	//fmt.Fprintf(stderr, peer_list);
}

// bootstrap methods
func calculate_and_inform(peer_id int) {
	int pre = 0;
	for (int i = 0; i < len(peer_list); i++) {
		if (peer_id > peer_list[i] && peer_list[i] > pre) {
			pre = peer_list[i];
		} 
	}
	int suc = 128;
	for (int i = 0; i < len(peer_list); i++) {
		if (peer_id < peer_list[i] && peer_list[i] < suc) {
			suc = peer_list[i];
		} 
	}
	var pd peerData := {
		id : peer_id,
		predecessor : pre,
		successor : suc
	}
	inform_peer(peer_id, pd);
	if (pre > 0) { 
	inform_peer(predecessor, pd);
	}
	if (suc < 128) {
	inform_peer(successor, pd);
	}
}


func inform_peer(peer_id int, pd peerData) {
	sendPeerData(peer_id, pd)
}

func request_peer(req request) {
	sendValue(1, req)
}

func send_client(req request) {
	sendValue(req.clientId, req)
}


// peer methods
func join(peer_id string) {
	var pd peerData := {
		id : int(peer_id),
		predecessor : -1,
		successor : -1
	}

	sendPeerData(server_name, pd)
}

func receive_peerData(pd peerData) {
	if (pd.id == myData.id) {
		myData = peerData {
			id : pd.id,
			predecessor : pd.predecessor,
			successor : pd.successor
		}
	}
	if (pd.predecessor == myData.id) {
		myData.successor == pd.id;
	}
	if (pd.successor == myData.id) {
		myData.predecessor = pd.id;
	}
}
func store_or_retrieve_object(req request) {
	if (req.op == STORE) {
		store_object(req)
	} 
	if (req.op == RETRIEVE) {
		retrieve_object(req)
	}
}

// retrive object from store
func retrieve_object(r request) {
	var req request

	for key, value := range objectsMap {
		if (r.objId == value && key == r.clientId) {
			// if value is found, it will be a success
			req = request{
				reqId : r.reqId,
				op : r.op
				objId : r.objId
				clientId : r.clientId,
				res : SUCCESS
			}

			sendValue(server_name, req)
			break;
			
		}
	}

	req = request{
		reqId : r.reqId,
		op : r.op
		objId : r.objId
		clientId : r.clientId,
		res : SUCCESS
	}

	sendValue(server_name, req)
}

func store_object {

}

// client methods
func sendRequest(value int) {
	var req request
	req = request {
		reqId : r.reqId,
		op : r.op
		objId : value
		clientId : r.clientId,
		res : NOTHING
	}
	sendValue(server_name, req)
}

func recvfrom_bootstrap(req request) {
	if (req.res == SUCCESS) {
		if (req.op == STORE) {
			fmt.Fprintf(stderr, "STORED: %d", req.objId)
		}
		if (req.op == RETRIEVE) {
			fmt.Fprintf(stderr, "RETRIEVED: %d", req.objId)
		}
	} 

	if (req.res == FAILURE) {
		if (req.op == RETRIEVE) {
			fmt.Fprintf(stderr, "NOT FOUND: %d", req.objId)

		}
	}
}


func main() {
		// command line inputs
		server_name = flag.String("b", "", "bootstrap server name")
		start_delay := flag.Int("d", 0, "the delay before starting up")
		store_name = flag.String("o", "", "object store filename")
		request_value = flag.Int("t", "", "the value that should be stored")
	
		flag.Parse()

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

		if (isClient) {
			sendRequest(request_value)
		}

		if (isPeer) {
			join(hostname)
		}



}


// processsing

func sendPeerData(peer_id string, pd PeerData) {
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

func requestToBytes(r request) []byte {
	return []byte(fmt.Sprintf("%d,%d,%d,%d", r.reqId, r.op, r.objId, r.clientId))
}

// Convert byte slice to proposal
func bytesToRequest(data []byte) proposal {
	var r request
	fmt.Sscanf(string(data), "%d,%d,%d,%d", &r.reqId, &r.op, &r.objId, &r.clientId)
	return r
}

func processBuffer(buffer []byte) {
	var data interface{}

	err := json.Unmarshal(buffer, &data)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err.Error())
		return
	}

	switch v := data.(type) {
	case map[string]interface{}:
		// Check if it's peerData or request based on the presence of specific keys
		if _, ok := v["id"]; ok {
			pd := peerData{
				id:          int(v["id"]),
				predecessor: int(v["predecessor"]),
				successor:   int(v["successor"]),
			}
		} else if _, ok := v["reqId"]; ok {
			req := request{
				reqId:    int(v["reqId"]),
				op:       operationType(v["op"]),
				objId:    int(v["objId"]),
				clientId: int(v["clientId"]),
				res:      responseType[v["res"]],
			}
			fmt.Println("Received request:", request)
		} else {
			fmt.Println("Unknown type received")
		}
	default:
		fmt.Println("Unknown type received")
	}
}

func receiveValue(connection net.Conn, peerName string) {
	defer connection.Close()

	// Read proposal from the connection
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	var currentRequest := bytesToRequest(buffer)

	if (isPeer) {
		if (currentRequest.objId >= peerData.id) {
			sendValue(peerData.successor, r)
		} else {
			store_or_retrieve_object(currentRequest)
		}
	}

	if (isBootstrap) {
		processBuffer(buffer)
		// needs if statement
		// if receives from client sends to first in peer list
		sendValue(peer_list[0], r)

		// if receives from response from peer list and sends response to client
		sendValue(req.clientId, req)

	}

	if (isClient) {
		recvfrom_bootstrap(currentRequest)
	}


}









func sendValue(peer_id int, req request) {
	var SERVER_TYPE = "tcp"

	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer+":"+PORT)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}

	requestBytes := requestToBytes(req)

	///send proposal
	_, err = connection.Write(requestBytes)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	defer connection.Close()
}

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

func openObjectFile(filename string) {
	file, err := os.Open(filename)
	checkIfError(err, "Error opening file:")

	defer file.Close()

	objectsMap = make(map[int]int)

		// Create a scanner to read the file line by line
		scanner := bufio.NewScanner(file)

		// Iterate over each line in the file
		for scanner.Scan() {
			line := scanner.Text()
	
			// Check for errors during scanning
			err := scanner.Err()
			checkIfError(err, "Error reading file:")

			parts := strings.Split(input, "::")

			// Create a map with the key-value pair
			objectsMap[parts[0]] = parts[1]

			
		}

}
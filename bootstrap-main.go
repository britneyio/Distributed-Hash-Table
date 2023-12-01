package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var (
	peerList  []int
	peerMap   map[int]PeerData
	clientMap map[int]string
)

func main() {
	var err error
	hostname, err = os.Hostname()
	checkIfError(err, "Error getting hostname:")

	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)

	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")

	peerMap = make(map[int]PeerData)
	clientMap = make(map[int]string)

	go listenBootstrap(listener)

	fmt.Fprintf(os.Stderr, "Started\n")

	select {}
}

// listens for the data
func listenBootstrap(listener net.Listener) {

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
		receiveValueBootStrap(connection, peerName[0])
	}
}

func receiveValueBootStrap(connection net.Conn, peerName string) {
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
		peerMap[currentData.Id] = currentData
		add_peer(strconv.Itoa(currentData.Id))

	}

	if _, ok := data["ReqId"]; ok {
		currentRequest = Request{
			ReqId:    int(data["ReqId"]),
			ClientId: int(data["ClientId"]),
			ObjId:    int(data["ObjId"]),
			Res:      ResponseType(data["Res"]),
			Op:       OperationType(data["Op"]),
		}
		// received from client if undefined
		if currentRequest.Res == UNDEFINED {
			clientMap[currentRequest.ClientId] = peerName
			sendRequest("n"+strconv.Itoa(peerMap[1].Id), currentRequest)
		} else {
			if currentRequest.Res == OBJ_STORED {
				currentRequest.Res = SUCCESS
			}
			sendRequest(clientMap[currentRequest.ClientId], currentRequest)
		}

	}
}

func add_peer(peer_Id string) {
	pId, err := strconv.Atoi(peer_Id)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return
	}
	peerList = append(peerList, pId)
	fmt.Fprintln(os.Stderr, "Peer list: ", peerList)
	//fmt.Fprintln(os.Stderr, "Peer map: ", peerMap)

	calculate_and_inform(peer_Id)

}

func maxList(numbers []int) int {
	if len(numbers) == 0 {
		return 0
	}

	max := numbers[0] // Assume the first element is the maximum

	for _, num := range numbers {
		if num > max {
			max = num
		}
	}

	return max
}

func calculate_and_inform(peer_Id string) {
	var pre int
	pre = 0
	pId, err := strconv.Atoi(peer_Id)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return
	}

	for i := 0; i < len(peerList); i++ {
		if pId > peerList[i] && peerList[i] > pre {
			pre = peerList[i]
		}
	}
	var suc int
	suc = 128
	for i := 0; i < len(peerList); i++ {
		if pId < peerList[i] && peerList[i] < suc {
			suc = peerList[i]
		}
	}

	// if peerId is the max of the List then its successor is peer 1
	if pId == maxList(peerList) {
		suc = 1
	}

	var pd = PeerData{
		Id:          pId,
		Predecessor: pre,
		Successor:   suc,
	}

	sendPeerData("n"+peer_Id, pd)
	if pre > 0 && pre < 128 {
		sendPeerData("n"+strconv.Itoa(pre), pd)
	}
	if suc < 128 && suc > 0 {
		sendPeerData("n"+strconv.Itoa(suc), pd)

	}
}

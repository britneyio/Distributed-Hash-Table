package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
)

type OperationType int64

const (
	STORE    OperationType = iota
	RETRIEVE OperationType = iota
	JOIN     OperationType = iota
)

type ResponseType int64

const (
	SUCCESS    ResponseType = iota
	FAILURE    ResponseType = iota
	UNDEFINED  ResponseType = iota
	OBJ_STORED ResponseType = iota
)

type PeerData struct {
	Id          int
	Predecessor int
	Successor   int
}

type Request struct {
	ReqId    int
	Op       OperationType
	ObjId    int
	ClientId int
	Res      ResponseType
}

var (
	storeName      *string
	serverName     *string
	hostname       string
	objectsMap     map[string][]int
	myData         PeerData
	PORT           = "4950"
	currentData    PeerData
	currentRequest Request
)

func checkIfError(e error, message string) {
	if e != nil {
		fmt.Fprintln(os.Stderr, message)
		//panic(e)
	}
}

// takes in a string for peer_Id ter and a Request and
// sends the Request to peer_Id (can be client, server, or peer)
func sendRequest(peer_Id string, req Request) {
	var SERVER_TYPE = "tcp"

	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer_Id+":"+PORT)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error Resolving addRess:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error connecting:", err)
		return
	}
	fmt.Fprintf(os.Stderr, "Sending Request - ReqId: %d, Op: %d, ObjId: %d, Res: %d, ClientId: %d\n", req.ReqId, req.Op, req.ObjId, req.Res, req.ClientId)
	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error marshalling JSON:", err)
		return
	}
	_, err = connection.Write(jsonData)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error reading:", err.Error())
	}
	defer connection.Close()
}

// takes in a string for peer_Id  and a PeerData and
// sends the PeerData to peer_Id (can be client, server, or peer)
func sendPeerData(peer_Id string, pd PeerData) {
	var SERVER_TYPE = "tcp"
	fmt.Fprintln(os.Stderr, "peerId:", peer_Id)
	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer_Id+":"+PORT)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error Resolving addRess:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error connecting:", err)
		return
	}

	//fmt.Fprintf(os.Stderr, "Sending PeerData - Id: %d, pre: %d, suc: %d\n", pd.Id, pd.Predecessor, pd.Successor)
	jsonData, err := json.Marshal(pd)
	//fmt.Fprintf(os.Stderr, "jsonData: %s", string(jsonData))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error marshalling JSON:", err)
		return
	}
	_, err = connection.Write(jsonData)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error reading:", err.Error())
	}
	defer connection.Close()
}

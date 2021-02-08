package main

import (
	"encoding/json"
	"net"
	"strings"

	"net/http"

	"time"
)

type BelugaExternlaNodeRequest struct {
	Id        string
	Topic     string
	Addr      string
	Port      int
	Method    string
	NatIPFlag string
}

type BelugaInfo struct {
	ContainerIP  string `json:"ContainerIP"`
	ExternalNat  string `json:"ExternalNat"`
	ExternalHost string `json:"ExternalHost"`
	ExternalPort int    `json:"ExternalPort"`
}

func getEthernetIP(inter string) string {

	var ip string
	ifi, err := net.InterfaceByName(inter)
	if err != nil {
		globals.logger.Infof("getEthernetIP InterfaceByName  ", err.Error())
		return ip
	}

	addrs, err := ifi.Addrs()
	if err != nil {
		globals.logger.Infof("getEthernetIP Addrs ", err.Error())
		return ip
	}

	for _, a := range addrs {
		ipaddr := strings.Split(a.String(), "/")
		if len(ipaddr) < 1 {
			continue
		}
		globals.logger.Infof("Interface %q, address %s", ifi.Name, ipaddr[0])
		ip = ipaddr[0]
	}

	return ip

}

func serveRest(wrt http.ResponseWriter, req *http.Request) {
	now := time.Now().UTC().Round(time.Millisecond)

	/*if isValid, _ := checkAPIKey(getAPIKey(req)); !isValid {
		wrt.WriteHeader(http.StatusForbidden)
		json.NewEncoder(wrt).Encode(ErrAPIKeyRequired(now))
		log.Println("ws: Missing, invalid or expired API key")
		return
	}*/

	if req.Method == http.MethodGet {
		var nat_ip string
		var host_ip string
		var port int
		if globals.cluster != nil {
			nat_ip = globals.cluster.nat_ip
			host_ip = globals.cluster.host_ip
			port = globals.cluster.public_port
		}

		beluga_info := &BelugaInfo{
			ContainerIP:  getEthernetIP("eth0"),
			ExternalNat:  nat_ip,
			ExternalHost: host_ip,
			ExternalPort: port,
		}
		wrt.WriteHeader(http.StatusOK)
		json.NewEncoder(wrt).Encode(beluga_info)

		return
	}
	if req.Method != http.MethodPost {
		wrt.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(wrt).Encode(ErrOperationNotAllowed("", "", now))
		globals.logger.Info("rest: Invalid HTTP method ", req.Method)
		return
	}

	var nodeReq BelugaExternlaNodeRequest

	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	err := json.NewDecoder(req.Body).Decode(&nodeReq)
	if err != nil {
		//http.Error(wrt, err.Error(), http.StatusBadRequest)
		wrt.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(wrt).Encode(ErrMalformed("", "", now))
		return
	}

	globals.logger.Info(nodeReq)
	if nodeReq.Method == "CREATE" {
		NewClusterTopic(nodeReq.Topic, nodeReq.Addr, nodeReq.Port, true, nodeReq.NatIPFlag)

		globals.logger.Info("rest: creat topic notify user natIPFlag=", nodeReq.NatIPFlag)

	} else if nodeReq.Method == "DELETE" {

		tp := LoadClusterTopic(nodeReq.Topic)
		if tp != nil {
			err := tp.Del()
			if err != nil {
				globals.logger.Warn("rest: delete topic ", err)
				wrt.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(wrt).Encode(ErrUnknown(nodeReq.Id, nodeReq.Topic, now))
				return
			}
		} else {
			wrt.WriteHeader(http.StatusNotFound)
			json.NewEncoder(wrt).Encode(ErrTopicNotFound(nodeReq.Id, nodeReq.Topic, now))
			return
		}

	} else {
		wrt.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(wrt).Encode(ErrMalformed(nodeReq.Id, nodeReq.Topic, now))
		return
	}
	//fmt.Fprintf(wrt, "%s", http.StatusText(http.StatusOK))
	wrt.WriteHeader(http.StatusOK)
	json.NewEncoder(wrt).Encode(NoErr(nodeReq.Id, nodeReq.Topic, now))

}

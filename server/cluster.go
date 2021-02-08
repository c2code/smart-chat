package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"encoding/json"
	"errors"

	//"fmt"
	"io/ioutil"
	"net"
	"net/rpc"

	//"sort"
	"strings"
	"sync"
	"time"

	"hexmeet.com/beluga/chat/server/auth"
	//rh "hexmeet.com/beluga/chat/server/ringhash"
	"strconv"

	"hexmeet.com/beluga/chat/server/store"
	"hexmeet.com/beluga/chat/server/store/types"
)

const (
	// Default timeout before attempting to reconnect to a node
	defaultClusterReconnect = 200 * time.Millisecond
	// Number of replicas in ringhash
	clusterHashReplicas = 20
)

type clusterNodeConfig struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

type clusterConfig struct {
	// List of all members of the cluster, including this member
	Nodes []clusterNodeConfig `json:"nodes"`
	// Name of this cluster node
	ThisName string `json:"self"`
	// Failover configuration
	//Failover *clusterFailoverConfig
}

// ClusterNode is a client's connection to another node.
type ClusterNode struct {
	lock sync.Mutex

	// RPC endpoint
	endpoint *rpc.Client
	// True if the endpoint is believed to be connected
	connected bool
	// True if a go routine is trying to reconnect the node
	reconnecting bool
	// TCP address in the form host:port
	address string
	// Name of the node
	//name string

	// A number of times this node has failed in a row
	failCount int

	// Channel for shutting down the runner; buffered, 1
	done chan bool

	//external ip of the node if it is cross domain req
	//addr string

	//Local Port, it is used when the request is within domain
	//port int

	//0 for internal domain request, 1 cross domian
	node_type int
}

// ClusterSess is a basic info on a remote session where the message was created.
type ClusterSess struct {
	// IP address of the client. For long polling this is the IP of the last poll
	RemoteAddr string

	// User agent, a string provived by an authenticated client in {login} packet
	UserAgent string

	// ID of the current user or 0
	Uid types.Uid

	// User's authentication level
	AuthLvl auth.Level

	// Protocol version of the client: ((major & 0xff) << 8) | (minor & 0xff)
	Ver int

	// Human language of the client
	Lang string

	// Device ID
	DeviceID string

	// Device platform: "web", "ios", "android"
	Platform string

	// Session ID
	Sid string
}

// ClusterReq is a Proxy to Master request message.
type ClusterReq struct {
	// Name of the node sending this request, local host name
	Node string

	//external ip of the node if it is cross domain req
	ExternalIP string

	//Local Port, it is used when the request is within domain
	LocalPort int

	//External Port,, it is used when the request is cross domain
	ExternalPort int
	NatIPFlag    string
	//0 for internal domain request, 1 cross domian
	Type int

	// Ring hash signature of the node sending this request
	// Signature must match the signature of the receiver, otherwise the
	// Cluster is desynchronized.
	Signature string

	Pkt *ClientComMessage

	StoreUsers     *types.User
	SyncStoreUsers bool

	// Root user may send messages on behalf of other users.
	OnBehalfOf string
	// AuthLevel of the user specified by root.
	AuthLvl int

	// Expanded (routable) topic name
	RcptTo string
	// Originating session
	Sess *ClusterSess
	// True if the original session has disconnected
	SessGone bool

	From string
}

// ClusterResp is a Master to Proxy response message.
type ClusterResp struct {
	Msg []byte
	// Session ID to forward message to, if any.
	FromSID string
}

// ClusterPing is content of a leader node ping to a follower node.
type ClusterPing struct {
}

// Handle outbound node communication: read messages from the channel, forward to remote nodes.
// FIXME(gene): this will drain the outbound queue in case of a failure: all unprocessed messages will be dropped.
// Maybe it's a good thing, maybe not.
func (n *ClusterNode) sync_connect(retry int) error {
	var reconnTicker *time.Ticker

	// Avoid parallel reconnection threads
	n.lock.Lock()
	if n.reconnecting {
		n.lock.Unlock()
		return nil
	}
	n.reconnecting = true
	n.lock.Unlock()

	var count = 0
	var err error
	//var err error
	for {
		if globals.rpcTlsEnabled == true {
			config := getTlsConfig(true)
			conn, err := tls.Dial("tcp", n.address, config)
			if err == nil {
				n.endpoint = rpc.NewClient(conn)
			}
		} else {
			n.endpoint, err = rpc.Dial("tcp", n.address)
		}
		if err == nil && n.endpoint != nil {
			if reconnTicker != nil {
				reconnTicker.Stop()
			}
			//fmt.Println(n.endpoint)
			n.lock.Lock()
			n.connected = true
			n.reconnecting = false
			n.lock.Unlock()

			globals.logger.Infof("cluster: connection to '%s' established %v", n.address, n.endpoint)
			return nil
		} else if count == 0 {
			reconnTicker = time.NewTicker(defaultClusterReconnect)
		}
		count++

		select {
		case <-reconnTicker.C:
			// Wait for timer to try to reconnect again. Do nothing if the timer is inactive.
			if count >= retry {
				return errors.New("connect to " + n.address + " timeout")
			}

		}
	}

	return nil
}

// Handle outbound node communication: read messages from the channel, forward to remote nodes.
// FIXME(gene): this will drain the outbound queue in case of a failure: all unprocessed messages will be dropped.
// Maybe it's a good thing, maybe not.
func (n *ClusterNode) reconnect() {
	defer TryLogDump()
	var reconnTicker *time.Ticker

	// Avoid parallel reconnection threads
	n.lock.Lock()
	if n.reconnecting {
		n.lock.Unlock()
		return
	}
	n.reconnecting = true
	n.lock.Unlock()

	var count = 0
	var err error
	for {
		if globals.rpcTlsEnabled == true {
			config := getTlsConfig(true)
			conn, err := tls.Dial("tcp", n.address, config)
			if err == nil {
				n.endpoint = rpc.NewClient(conn)
			}
		} else {
			n.endpoint, err = rpc.Dial("tcp", n.address)
		}
		// Attempt to reconnect right away
		if err == nil && n.endpoint != nil {
			if reconnTicker != nil {
				reconnTicker.Stop()
			}
			n.lock.Lock()
			n.connected = true
			n.reconnecting = false
			n.lock.Unlock()

			globals.logger.Infof("cluster: reconnection to '%s' established, endpoint %v", n.address, n.endpoint)
			/*
				iterate session store and notify ws to reconnect
			*/
			globals.sessionStore.EvictSession(n.address)
			return
		} else if count == 0 {
			reconnTicker = time.NewTicker(defaultClusterReconnect)
		}

		count++

		select {
		case <-reconnTicker.C:
			// Wait for timer to try to reconnect again. Do nothing if the timer is inactive.
		case <-n.done:
			// Shutting down
			globals.logger.Infof("cluster: node '%s' shutdown started", n.address)
			reconnTicker.Stop()
			if n.endpoint != nil {
				n.endpoint.Close()
				n.endpoint = nil //added by yiye
			}
			n.lock.Lock()
			n.connected = false
			n.reconnecting = false
			n.lock.Unlock()
			globals.logger.Infof("cluster: node '%s' shut down completed", n.address)
			return
		}
	}
}

func (n *ClusterNode) call(proc string, msg, resp interface{}) error {
	if !n.connected {
		globals.logger.Infof("cluster: node '%s' not connected ", n.address)
		return errors.New("cluster: node '" + n.address + "' not connected")
	}

	if err := n.endpoint.Call(proc, msg, resp); err != nil {
		globals.logger.Infof("cluster: call failed to '%s' [%s]", n.address, err)

		n.lock.Lock()
		if n.connected {
			n.endpoint.Close()
			n.connected = false
			go n.reconnect()
		}
		n.lock.Unlock()
		return err
	}

	return nil
}

func (n *ClusterNode) callAsync(proc string, msg, resp interface{}, done chan *rpc.Call) *rpc.Call {
	if done != nil && cap(done) == 0 {
		globals.logger.Panic("cluster: RPC done channel is unbuffered")
	}

	if !n.connected {
		call := &rpc.Call{
			ServiceMethod: proc,
			Args:          msg,
			Reply:         resp,
			Error:         errors.New("cluster: node '" + n.address + "' not connected"),
			Done:          done,
		}
		if done != nil {
			done <- call
		}
		return call
	}

	myDone := make(chan *rpc.Call, 1)
	go func() {
		call := <-myDone
		if call.Error != nil {
			n.lock.Lock()
			if n.connected {
				n.endpoint.Close()
				n.connected = false
				go n.reconnect()
			}
			n.lock.Unlock()
		}

		if done != nil {
			done <- call
		}
	}()

	call := n.endpoint.Go(proc, msg, resp, myDone)
	call.Done = done

	return call
}

// Proxy forwards message to master
func (n *ClusterNode) forward(msg *ClusterReq) error {
	globals.logger.Infof("cluster: forwarding request to node '%s'", n.address)
	msg.Node = globals.cluster.thisNodeName
	rejected := false
	err := n.call("Cluster.Master", msg, &rejected)
	if err == nil && rejected {
		err = errors.New("cluster: master node out of sync")
	}
	return err
}

// Master responds to proxy
func (n *ClusterNode) respond(msg *ClusterResp) error {
	globals.logger.Infof("cluster: replying to node '%s'", n.address)
	unused := false
	return n.call("Cluster.Proxy", msg, &unused)
}

func (n *ClusterNode) shutdown() {
	globals.logger.Infof("Cluster %s shut down", n.address)
	n.done <- true
}

// Cluster is the representation of the cluster.
type Cluster struct {
	// Cluster nodes with RPC endpoints (excluding current node).
	nodes map[string]*ClusterNode
	// Name of the local node
	thisNodeName string

	public_ip   string
	public_port int

	host_ip   string
	nat_ip    string
	NatIPFlag string
	// Resolved address to listed on
	listenPort int

	//listenAddr	string
	// Socket for inbound connections
	inbound net.Listener
	// Ring hash for mapping topic names to nodes
	//ring *rh.Ring

	// Failover parameters. Could be nil if failover is not enabled
	//fo *clusterFailover
	lock sync.Mutex
}

func (c *Cluster) getNodeNameFromReq(msg *ClusterReq) string {

	var node_name string
	if msg.Type == LOCAL_TOPIC {
		node_name = msg.Node + ":" + strconv.Itoa(msg.LocalPort)
	} else {
		node_name = msg.ExternalIP + ":" + strconv.Itoa(msg.ExternalPort)
	}

	return node_name
}

// Master at topic's master node receives C2S messages from topic's proxy nodes.
// The message is treated like it came from a session: find or create a session locally,
// dispatch the message to it like it came from a normal ws/lp connection.
// Called by a remote node.
func (c *Cluster) Master(msg *ClusterReq, rejected *bool) error {
	globals.logger.Infof("cluster: Master request received from node '%s' ,sssion id '%s'", msg.Node, msg.Sess.Sid)

	// Find the local session associated with the given remote session.
	sess := globals.sessionStore.Get(msg.Sess.Sid)
	//write to redis
	globals.cluster.NatIPFlag = msg.NatIPFlag
	if msg.NatIPFlag == "true" {
		globals.cluster.public_ip = globals.cluster.nat_ip
	} else {
		globals.cluster.public_ip = globals.cluster.host_ip
	}
	if msg.SessGone {
		// Original session has disconnected. Tear down the local proxied session.
		if sess != nil {
			globals.logger.Info("cluster: session gone del users %s", sess.uid)
			sess.stop <- nil
		}
	} else {
		// This cluster member received a request for a topic it owns.

		if sess == nil {
			// If the session is not found, create it
			node_name := c.getNodeNameFromReq(msg)
			globals.logger.Info("cluster: request from an node ", node_name)
			var node *ClusterNode
			c.lock.Lock()
			node = globals.cluster.nodes[node_name]
			c.lock.Unlock()
			if node == nil {
				globals.logger.Info("cluster: request from an unknown node ", node_name)

				node = &ClusterNode{
					address: node_name,
					//port:    clustertopic.port,
					node_type: msg.Type,
					done:      make(chan bool, 1)}
				//n.reconnect()
				err := node.sync_connect(5)
				if err != nil {
					//rejected = true
					return err
				}
				globals.logger.Infof("cluster: add node %s", node_name)
				globals.cluster.addNode(node_name, node)
				//return nil
			}

			sess, _ = globals.sessionStore.NewSession(node, msg.Sess.Sid)

			if CROSS_DOMAIN_TOPIC == msg.Type {
				cluser_user := NewClusterUser(msg.From, "on", msg.ExternalIP, msg.ExternalPort)
				sess.cluster_user = cluser_user
			}
			//add userid mapping extenal ip and port, node_name, type
			go sess.rpcWriteLoop()
		}

		// Update session params which may have changed since the last call.
		sess.uid = msg.Sess.Uid
		sess.authLvl = msg.Sess.AuthLvl
		sess.ver = msg.Sess.Ver
		sess.userAgent = msg.Sess.UserAgent
		sess.remoteAddr = msg.Sess.RemoteAddr
		sess.lang = msg.Sess.Lang
		sess.deviceID = msg.Sess.DeviceID
		sess.platf = msg.Sess.Platform

		// Dispatch remote message to a local session.
		msg.Pkt.from = msg.OnBehalfOf
		msg.Pkt.authLvl = msg.AuthLvl

		//oper user
		operStoreUsers(msg.StoreUsers, msg.SyncStoreUsers)
		sess.dispatch(msg.Pkt)
	}

	return nil
}

// Proxy receives messages from the master node addressed to a specific local session.
// Called by Session.writeRPC
func (Cluster) Proxy(msg *ClusterResp, unused *bool) error {
	globals.logger.Info("cluster: Proxy response from Master for session ", msg.FromSID)

	// This cluster member received a response from topic owner to be forwarded to a session
	// Find appropriate session, send the message to it
	if sess := globals.sessionStore.Get(msg.FromSID); sess != nil {
		//conver []byte to
		var rsp ServerComMessage
		if err := json.Unmarshal(msg.Msg, &rsp); err != nil {
			// Malformed message
			//s.logger.Info("s.dispatch", err, s.sid)
			globals.logger.Info("cluster: Proxy not valid json msg ", err)
			sess.queueOut(ErrMalformed("", "", time.Now().UTC().Round(time.Millisecond)))
			return err
		}

		if !sess.queueOut(&rsp) {
			globals.logger.Info("cluster.Proxy: timeout")
		}
	} else {
		globals.logger.Info("cluster: master response for unknown session ", msg.FromSID)
	}

	return nil
}

func routeToGrpCluster(sess *Session) *string {
	//sess.subs get grpxxx topic
	var findFlag bool
	var findAddr string
	findFlag = false
	for addr, val := range sess.nodes {
		globals.logger.Infof("routeToGrpCluster: range sess.nodes addr %s", addr)
		if true == val {
			findFlag = true
			findAddr = addr
			break
		}
	}
	if false == findFlag {
		return nil
	}
	return &findAddr
}

// Given topic name, find appropriate cluster node to route message to
func (c *Cluster) nodeConnect(nodeName string, nodeType int, topic string) (*ClusterNode, error) {
	var node *ClusterNode
	c.lock.Lock()
	node = globals.cluster.nodes[nodeName]
	c.lock.Unlock()
	if node == nil {
		globals.logger.Infof("cluster: no node for topic %s %s", topic, nodeName)
		//creat node
		node = &ClusterNode{
			address:   nodeName,
			node_type: nodeType,
			done:      make(chan bool, 1)}
		err := node.sync_connect(5)
		if err != nil {
			return nil, err
		}
		globals.logger.Infof("cluster: add node %s ", nodeName)
		globals.cluster.addNode(nodeName, node)
		return node, nil
	}
	return node, nil
}

func (c *Cluster) nodeForTopic(topic string, originalTopic string, sess *Session) (*ClusterNode, error) {
	var clustertopic *ClusterTopic = nil
	var nodeName string
	var nodeType int

	if strings.HasPrefix(topic, "usr") || strings.HasPrefix(topic, "p2p") {
		tmpTopic := getOtherUserFromP2PTopic(topic, originalTopic)
		clusterUser := LoadUser(tmpTopic)
		if nil == clusterUser {
			tmpNode := routeToGrpCluster(sess)
			if nil == tmpNode {
				globals.logger.Infof("cluster: routeToGrpCluster not found node %s", topic)
				return nil, errors.New("cluster: nrouteToGrpCluster not found node " + topic)
			}
			nodeName = *tmpNode
			nodeType = CROSS_DOMAIN_TOPIC
		} else {
			nodeName = clusterUser.GetUserAddr()
			nodeType = clusterUser.user_type
		}
		return c.nodeConnect(nodeName, nodeType, topic)
	} else if strings.Contains(topic, "grp") {
		clustertopic = LoadClusterTopic(topic)
	} else {
		globals.logger.Infof("nodeForTopic: type error topic %s", topic)
	}
	if nil == clustertopic {
		globals.logger.Infof("cluster: node topic not found in redis %s", topic)
		return nil, errors.New("cluster: node topic not found in redis " + topic)
	}
	nodeName = clustertopic.GetClusterNodeAddr()
	nodeType = clustertopic.topic_type
	//reload natipflag
	globals.cluster.NatIPFlag = clustertopic.natIPFlag

	return c.nodeConnect(nodeName, nodeType, topic)
}

func getOtherUserFromP2PTopic(topic string, originalTopic string) string {
	if !strings.HasPrefix(topic, "p2p") {
		return topic
	}
	var tmpTopic string
	if uid1, uid2, err := types.ParseP2P(topic); err == nil {
		// If this is a P2P topic, index it by second user's ID
		if uid1.UserId() == originalTopic {
			tmpTopic = uid2.UserId()
		} else {
			tmpTopic = uid1.UserId()
		}
		return tmpTopic
	}
	return topic
}

func (c *Cluster) isRemoteUser(topic string, originalTopic string) bool {

	// LoadUser (ClusterUser)
	// if it is a local user
	// if yes, compare the host with local hostName
	// if host match, return false, else return true
	// if it is remote, return true
	//if not found, then check the self userid sub, find the grpid in sub
	//get the grouid ,and LoadClusterTopic to check if it is remote

	var tmpTopic string
	if strings.HasPrefix(topic, "p2p") {
		tmpTopic = getOtherUserFromP2PTopic(topic, originalTopic)
	} else {
		tmpTopic = topic
		globals.logger.Info("isRemoteUser oper topic %s", topic)
	}
	clusterUser := LoadUser(tmpTopic)
	//have to ask remote where grpxxx create
	if nil == clusterUser {
		return true
	}
	if clusterUser.host == c.thisNodeName &&
		clusterUser.port == c.listenPort &&
		LOCAL_USER == clusterUser.user_type {

		return false
	}
	return true
}

func (c *Cluster) isRemoteGrpTopic(topic string) bool {
	clustertopic := LoadClusterTopic(topic)
	//redis get none
	if nil == clustertopic {
		return false
	}

	if clustertopic.node_name == c.thisNodeName &&
		clustertopic.port == c.listenPort &&
		LOCAL_TOPIC == clustertopic.topic_type {

		return false
	}
	return true
}

// isRemoteTopic checks if the given topic is handled by this node or a remote node.
func (c *Cluster) isRemoteTopic(topic string, originalTopic string) bool {
	if c == nil {
		// Cluster not initialized, all topics are local
		return false
	}
	//if it starts with p2p or usr
	if strings.HasPrefix(topic, "p2p") {
		//isLocalP2PChat
		//return true
		rsp := make(chan bool)
		globals.logger.Infof("cluster query hub topic %s request", topic)
		globals.hub.get <- &topicQuery{
			topic:   topic,
			existed: rsp}
		select {
		case bexisted := <-rsp:
			if bexisted == true {
				return false
			}
			break
		}
		return c.isRemoteUser(topic, originalTopic)
	} else if strings.Contains(topic, "grp") {
		return c.isRemoteGrpTopic(topic)
	}
	return false
}

func getStoreUsers(msg *ClientComMessage, topic string, uid types.Uid) (*types.User, bool) {
	var operFlag = false
	if !strings.Contains(topic, "grp") {
		return nil, operFlag
	}

	clustertopic := LoadClusterTopic(topic)
	if clustertopic == nil {
		return nil, operFlag
	}

	if clustertopic.topic_type != CROSS_DOMAIN_TOPIC {
		return nil, operFlag
	}

	if msg.Del != nil || msg.Leave != nil {
		return nil, operFlag
	}
	globals.logger.Info("getStoreUsers: load user object for topic %s uid %s operflag %v", topic, uid, operFlag)
	user, err := store.Users.Get(uid)
	if err != nil {
		globals.logger.Info("getStoreUsers: cannot load user object for (" + err.Error() + ")")
		return nil, operFlag
	} else if user == nil {
		globals.logger.Info("getStoreUsers: user's account unexpectedly not found (deleted?)")
		return nil, operFlag
	}
	operFlag = true
	return user, operFlag
}

func operStoreUsers(userInfo *types.User, bSyncUser bool) {
	globals.logger.Infof("operStoreUsers: in")
	if userInfo == nil {
		return
	}
	type private struct {
		Comment string `json:"comment"`
	}
	req := private{Comment: "cluster user"}
	if bSyncUser == true {
		if _, err := store.Users.Create(userInfo, req); err != nil {
			globals.logger.Infof("operStoreUsers: failed to create user %s", err.Error())
			return
		}
	}
	return
}

// Forward client message to the Master (cluster node which owns the topic)
func (c *Cluster) routeToTopic(msg *ClientComMessage, topic string, sess *Session) error {
	//globals.logger.Infof("routeToTopic in topic='%s' ", topic)
	// Find the cluster node which owns the topic, then forward to it.

	//if it starts with p2p or usr
	//if it is found in redis, create a node
	//if it is usr's subs
	n, errs := c.nodeForTopic(topic, msg.from, sess)
	if n == nil {
		return errs
	}
	globals.logger.Infof("routeToTopic: node topic redis %s,globals.cluster.NatIPFlag %v", topic, globals.cluster.NatIPFlag)
	if globals.cluster.NatIPFlag == "true" {
		globals.cluster.public_ip = globals.cluster.nat_ip
	} else {
		globals.cluster.public_ip = globals.cluster.host_ip
	}
	//add users info
	users, operFlag := getStoreUsers(msg, topic, sess.uid)
	// Save node name: it's need in order to inform relevant nodes when the session is disconnected
	if sess.nodes == nil {
		sess.nodes = make(map[string]bool)
	}
	globals.logger.Infof("routeToTopic topic='%s' node address %s", topic, n.address)
	sess.nodes[n.address] = true

	req := &ClusterReq{
		Node:      c.thisNodeName,
		NatIPFlag: c.NatIPFlag,
		SessGone:  false,
		//Signature: c.ring.Signature(),
		Pkt:            msg,
		RcptTo:         topic,
		StoreUsers:     users,
		SyncStoreUsers: operFlag,
		Type:           n.node_type,
		LocalPort:      globals.cluster.listenPort,
		ExternalIP:     globals.cluster.public_ip,
		ExternalPort:   globals.cluster.public_port,
		From:           msg.from,
		Sess: &ClusterSess{
			Uid:        sess.uid,
			AuthLvl:    sess.authLvl,
			RemoteAddr: sess.remoteAddr,
			UserAgent:  sess.userAgent,
			Ver:        sess.ver,
			Lang:       sess.lang,
			DeviceID:   sess.deviceID,
			Platform:   sess.platf,
			Sid:        sess.sid}}

	if sess.authLvl == auth.LevelRoot {
		// Assign these values only when the sender is root
		req.OnBehalfOf = msg.from
		req.AuthLvl = msg.authLvl
	}
	if req.StoreUsers == nil {
		globals.logger.Info("req.StoreUsers: is nil topic is %s", topic)
	}

	return n.forward(req)

}

// Session terminated at origin. Inform remote Master nodes that the session is gone.
func (c *Cluster) sessionGone(sess *Session) error {
	if c == nil {
		return nil
	}

	// Save node name: it's need in order to inform relevant nodes when the session is disconnected
	for name := range sess.nodes {
		n := c.nodes[name]
		if n != nil {
			return n.forward(
				&ClusterReq{
					Node:     c.thisNodeName,
					SessGone: true,
					Sess: &ClusterSess{
						Uid:        sess.uid,
						RemoteAddr: sess.remoteAddr,
						UserAgent:  sess.userAgent,
						Ver:        sess.ver,
						Sid:        sess.sid}})
		}
	}
	return nil
}

// Returns snowflake worker id
func clusterInit(configString json.RawMessage, self *string, localPort *int, hostAddr *string, natAddr *string, workerIdInput *int) int {
	if globals.cluster != nil {
		globals.logger.Fatal("Cluster already initialized.")
	}

	// This is a standalone server, not initializing
	if len(configString) == 0 {
		globals.logger.Info("Running as a standalone server.")
		return 1
	}

	var config clusterConfig
	if err := json.Unmarshal(configString, &config); err != nil {
		globals.logger.Fatal(err)
	}

	thisName := *self
	if thisName == "" {
		thisName = config.ThisName
	}

	// Name of the current node is not specified - disable clustering
	if thisName == "" {
		globals.logger.Info("Running as a standalone server.")
		return 1
	}

	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	globals.cluster = &Cluster{
		thisNodeName: thisName,
		listenPort:   *localPort,
		//public_ip:	*externalAddr,
		public_port: *localPort,
		host_ip:     *hostAddr,
		nat_ip:      *natAddr,

		nodes: make(map[string]*ClusterNode)}

	if nil == workerIdInput {
		globals.logger.Info("workerIdInput input is nil.")
		return 1
	}
	return *workerIdInput
	/*var nodeNames []string
	for _, host := range config.Nodes {
		nodeNames = append(nodeNames, host.Name)

		if host.Name == thisName {
			globals.cluster.listenOn = host.Addr
			// Don't create a cluster member for this local instance
			continue
		}

		globals.cluster.nodes[host.Name] = &ClusterNode{
			address: host.Addr,
			name:    host.Name,
			done:    make(chan bool, 1)}
	}

	if len(globals.cluster.nodes) == 0 {
		// Cluster needs at least two nodes.
		log.Fatal("Invalid cluster size: 1")
	}*/

	//sort.Strings(nodeNames)
	//workerId := sort.SearchStrings(nodeNames, thisName) + 1

	//return workerIdInput
}

// This is a session handler at a master node: forward messages from the master to the session origin.
func (sess *Session) rpcWriteLoop() {
	defer TryLogDump()
	// There is no readLoop for RPC, delete the session here
	defer func() {
		globals.logger.Info("writeRPC - stop")
		sess.closeRPC()
		globals.sessionStore.Delete(sess)
		if sess.cluster_user != nil {
			sess.cluster_user.DelUser()
		}
		sess.unsubAll()
	}()

	for {
		select {
		case msg, ok := <-sess.send:
			if !ok || sess.clnode.endpoint == nil {
				// channel closed
				globals.logger.Infof("sess.writeRPC chan failure sid:%s", sess.sid)
				return
			}
			// The error is returned if the remote node is down. Which means the remote
			// session is also disconnected.
			if err := sess.clnode.respond(&ClusterResp{Msg: msg.([]byte), FromSID: sess.sid}); err != nil {

				globals.logger.Infof("sess.writeRPC: %s" + err.Error())
				return
			}
		case msg := <-sess.stop:
			// Shutdown is requested, don't care if the message is delivered
			globals.logger.Infof("sess stop sid %s", sess.sid)
			if msg != nil {
				sess.clnode.respond(&ClusterResp{Msg: msg.([]byte), FromSID: sess.sid})
			}
			return

		case topic := <-sess.detach:
			globals.logger.Infof("sess detach sid %s, topic %s", sess.sid, topic)
			sess.delSub(topic)
		}
	}
}

// Proxied session is being closed at the Master node
func (sess *Session) closeRPC() {
	if sess.proto == CLUSTER {
		globals.logger.Info("cluster: session closed at master")
	}
}

func getTlsConfig(isClient bool) *tls.Config {

	var certFile string
	var keyFile string
	if isClient {
		certFile = globals.rpcTlsCertPath + "/client.crt"
		keyFile = globals.rpcTlsCertPath + "/client.key"
	} else {
		certFile = globals.rpcTlsCertPath + "/server.crt"
		keyFile = globals.rpcTlsCertPath + "/server.key"
	}
	caFile := globals.rpcTlsCertPath + "/ca.crt"

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		globals.logger.Fatal("server: loadkeys: %s", err)
	}
	pool := x509.NewCertPool()
	certBytes, err := ioutil.ReadFile(caFile)
	if err != nil {
		globals.logger.Fatalf("server: read certs/server ca.crt: %s", err)
	}
	ok := pool.AppendCertsFromPEM(certBytes)
	if !ok {
		panic("failed to parse root certificate")
	}
	var config *tls.Config
	if isClient {
		config = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			ServerName:   "server", //commonName of server.crt
		}
	} else {

		config = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    pool,
		}
	}
	return config
}

func (c *Cluster) start() {
	listenOn := ":" + strconv.Itoa(c.listenPort)
	var err error
	if true == globals.rpcTlsEnabled {
		config := getTlsConfig(false)
		c.inbound, err = tls.Listen("tcp", listenOn, config)
		if err != nil {
			globals.logger.Fatal(err)
		}
		if err != nil {
			globals.logger.Fatal(err)
		}
	} else {
		addr, err := net.ResolveTCPAddr("tcp", listenOn)
		c.inbound, err = net.ListenTCP("tcp", addr)
		if err != nil {
			globals.logger.Fatal(err)
		}
	}

	for _, n := range c.nodes {
		go n.reconnect()
	}

	go c.run()

	err = rpc.Register(c)
	if err != nil {
		globals.logger.Fatal(err)
	}

	go rpc.Accept(c.inbound)

	globals.logger.Infof("Cluster of %d nodes initialized, node '%s' listening on [%s]", len(globals.cluster.nodes)+1,
		globals.cluster.thisNodeName, listenOn)
}

func (c *Cluster) shutdown() {
	if globals.cluster == nil {
		return
	}
	globals.cluster = nil

	c.inbound.Close()

	for _, n := range c.nodes {
		n.done <- true
	}

	globals.logger.Info("Cluster shut down")
}

func (c *Cluster) Ping(ping *ClusterPing, unused *bool) error {

	return nil
}

func (c *Cluster) run() {
	defer TryLogDump()
	ticker := time.NewTicker(5000 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			c.sendPings()
		}
	}
}

func (c *Cluster) addNode(nodename string, node *ClusterNode) {
	c.lock.Lock()
	c.nodes[nodename] = node
	c.lock.Unlock()
}

func (c *Cluster) deleteNode(node_name string) {

	globals.logger.Infof("deleteNode %s", node_name)
	c.lock.Lock()
	n := c.nodes[node_name]
	if n != nil {
		n.shutdown()
	}
	delete(c.nodes, node_name)
	c.lock.Unlock()
}

func (c *Cluster) sendPings() {

	c.lock.Lock()
	newMap := make(map[string]*ClusterNode)
	for k, v := range c.nodes {
		newMap[k] = v
	}
	c.lock.Unlock()

	for _, node := range newMap {
		unused := false
		err := node.call("Cluster.Ping", &ClusterPing{}, &unused)

		if err != nil {
			node.failCount++
			globals.logger.Infof("cluster: ping %s failure %d", node.address, node.failCount)
			//if node.failCount == c.fo.nodeFailCountLimit {
			//	// Node failed too many times
			//	rehash = true
			//}
			if node.failCount > 5 {
				globals.logger.Warnf("cluster: ping delete dead node %s", node.address)
				c.lock.Lock()
				node.shutdown()
				delete(c.nodes, node.address)
				c.lock.Unlock()
				return
			}
		} else {

			//if node.failCount >= c.fo.nodeFailCountLimit {
			// Node has recovered
			//	rehash = true
			//}
			node.failCount = 0
		}
	}

}

package main

import (
	"strconv"
	"strings"
)

const (
    LOCAL_TOPIC int = iota //0
    CROSS_DOMAIN_TOPIC
)

const DELIMIT = "##"

type ClusterTopic struct {

	node_name 		string
	port	 		int //
	topic_type		int
	groupid			string
	natIPFlag       string
}

func NewClusterTopic(_groupid string, _node_name string ,_port int,  isCrossDomain bool, _natIPFlag string)  (*ClusterTopic){

	globals.logger.Infof("cluster_topic: NewClusterTopic groupid %s, node name %s, port %d, isCrossDomain %v, natIPFlag %s", _groupid, _node_name, _port,isCrossDomain,_natIPFlag)

	_topic_type := LOCAL_TOPIC
	if isCrossDomain {
		_topic_type = CROSS_DOMAIN_TOPIC;
	}
	t := &ClusterTopic{
		node_name:	_node_name,
		port	:	_port,
		topic_type: _topic_type,
		groupid	:	_groupid,
		natIPFlag: _natIPFlag,
	};
	t.Save()

	return t
}

func LoadClusterTopic(_groupid string) (*ClusterTopic){

	val, err := globals.redisConn.ReadChatGroup(_groupid)
	if err != nil {
		//panic(err)
		globals.logger.Infof("cluster_topic: LoadClusterTopic, not found topic %s from redis", _groupid)
		return nil
    } else {
		//fmt.Println("key2", val2)
		vals := strings.Split(val,DELIMIT)
		if len(vals) > 2 {
			addrs := strings.Split(vals[0],":")
			var addr string
			var _port int
			if len(addrs) > 1 {
				addr = addrs[0]
				_port,_ = strconv.Atoi(addrs[1])
			}
			if len(addr) > 0 {
				_topic_type,_ := strconv.Atoi(vals[1])
				_natIPFlag := vals[2]
				t := &ClusterTopic{
					node_name:	addr,
					port	:	_port,
					topic_type: _topic_type,
					groupid	:	_groupid,
					natIPFlag: _natIPFlag,
				};
				return t
			}
		}else{
			globals.logger.Infof("cluster_topic: LoadClusterTopic,topic %s,value len %d ", _groupid,len(vals))
		}

	}
	
	return nil;

}

func (tp *ClusterTopic) IsCrossDomainTopic() bool {

	if tp.topic_type == CROSS_DOMAIN_TOPIC {

		return true
	}

	return false
}

func (tp *ClusterTopic)  GetClusterNodeAddr() string{
	
	value := tp.node_name + ":" + strconv.Itoa(tp.port)

	return value
}

func (tp *ClusterTopic) GetClusterNodeName() string{

	return tp.node_name
}

func (tp *ClusterTopic) GetClusterNodePort() int {

	return tp.port
}

func (tp *ClusterTopic) Save() error {
	value := tp.node_name + ":" + strconv.Itoa(tp.port) + DELIMIT + strconv.Itoa(tp.topic_type)+ DELIMIT + tp.natIPFlag
	globals.logger.Infof("cluster_topic: Save, save topic %s -> %s", tp.groupid, value)
	return globals.redisConn.WriteChatGroup(tp.groupid, value)
}

func (tp *ClusterTopic) Del() error {
	globals.logger.Infof("cluster_topic: Del, del topic %s from redis", tp.groupid)
	return globals.redisConn.DeleteChatGroup(tp.groupid)
}

func DeleteClusterTopic(topic string)  error{

	return globals.redisConn.DeleteChatGroup(topic)
	
}
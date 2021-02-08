package main

import(
	"strconv"
	"strings"
)

const (
    LOCAL_USER int = iota //0
    CROSS_DOMAIN_USER
)


type ClusterUser struct {

	host 			string
	port	 		int //
	usrId			string
	status			string
	user_type		int
}

func NewClusterUser(userId string, userStatus string, clusterAddr string, clusterPort int) (*ClusterUser){

	if globals.cluster == nil {
		return nil
	}

	if len(globals.cluster.thisNodeName) == 0 {
		//globals.logger.Info("Running as a standalone server.")
		return nil
	}

	user := &ClusterUser{
		host:	clusterAddr,
		port:	clusterPort,
		usrId:	userId,
		status: userStatus,
		user_type: CROSS_DOMAIN_USER,
	}

	if userStatus == "on" {
		user.SetUserOnline()
	}else {
		user.SetUserOffline()
	}

	return user
}


func NewLocalUser(userId string, userStatus string) (*ClusterUser){
	
	if globals.cluster == nil {
		return nil
	}

	if len(globals.cluster.thisNodeName) == 0 {
		//globals.logger.Info("Running as a standalone server.")
		return nil
	}

	user := &ClusterUser{
		host:	globals.cluster.thisNodeName,
		port:	globals.cluster.listenPort,
		usrId:	userId,
		status: userStatus,
		user_type: LOCAL_USER,
	}

	if userStatus == "on" {
		user.SetUserOnline()
	}else {
		user.SetUserOffline()
	}

	return user
}


func LoadUser(userId string)  (*ClusterUser){
	if len(globals.cluster.thisNodeName) == 0 {
		//globals.logger.Info("Running as a standalone server.")
		return nil
	}

	val, err := globals.redisConn.ReadChatUser(userId)
	if err != nil {
		//panic(err)
		globals.logger.Infof("cluster_user: LoadClusterUser, not found user %s from redis", userId)
		return nil
	}
	//globals.redisConn.
	globals.redisConn.ReadChatUser(userId)
	vals := strings.Split(val,DELIMIT)
	if len(vals) > 3 {
		
		_status := vals[0]
		_host := vals[1]
		_port,_ := strconv.Atoi(vals[2])
		_user_type,_ := strconv.Atoi(vals[3])

		user := &ClusterUser{
			usrId: userId,
			status: _status,
			host: _host,
			port: _port,
			user_type: _user_type,
		}

		return user
	}

	return nil
}

func (user* ClusterUser)  SetUserOnline() error{

	globals.logger.Infof("cluster_user: SetUserOnline, user %s -> %s", user.usrId, "on")
	return globals.redisConn.WriteChatUser(user.usrId,"on" + DELIMIT + user.host + DELIMIT + strconv.Itoa(user.port) + DELIMIT + strconv.Itoa(user.user_type))
}

func (user* ClusterUser)  SetUserOffline() error{

	globals.logger.Infof("cluster_user: SetUserOffline, user %s -> %s", user.usrId, "off")
	return globals.redisConn.WriteChatUser(user.usrId,"off" + DELIMIT + user.host + DELIMIT + strconv.Itoa(user.port) +  DELIMIT + strconv.Itoa(user.user_type))
}

func DelClusterUser(userid string) error{
	
	if globals.cluster == nil {
		return nil
	}

	if len(globals.cluster.thisNodeName) == 0 {
		//globals.logger.Info("Running as a standalone server.")
		return nil
	}

	globals.logger.Infof("cluster_user: DelUser, user %s", userid)
	return globals.redisConn.DeleteChatUser(userid)
}

func (user* ClusterUser)  DelUser() error{	
	globals.logger.Infof("cluster_user: DelUser, user %s -> %s", user.usrId, "drop")
	return globals.redisConn.DeleteChatUser(user.usrId)
}

func (user* ClusterUser)GetHost()  string{
	return user.host
}

func (user* ClusterUser)GetPort()  int{
	return user.port
}

func (user* ClusterUser)GetStatus()  string{
	return user.status
}

func (user* ClusterUser)GetUserId()  string{
	return user.usrId
}

func (user* ClusterUser)GetUserType()  int{
	return user.user_type
}
func (user* ClusterUser)  GetUserAddr() string{

	value := user.host + ":" + strconv.Itoa(user.port)

	return value
}

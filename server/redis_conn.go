package main
import (
    "context"
	"github.com/go-redis/redis/v8"
    "errors"
    "strconv"
    "time"
)


const (
    STAND_ALONE int = iota //0
    SENTINEL
)

const (
	defaultPingTimer = 5000 * time.Millisecond
)



type RedisConn struct  {
   // ctx*         context.Context
    key_prefix         string
    rdb         *redis.Client
}

var ctx = context.Background()

func  RedisNewConn(service_name string, port int, mode int) *RedisConn {

    globals.logger.Infof("redis_conn: RedisNewConn, service name %s, port %d, mode %d", service_name, port, mode)
    var conn *redis.Client
    if mode == STAND_ALONE {
        conn = redis.NewClient(&redis.Options{
            Addr:     service_name + ":" + strconv.Itoa(port),
            Password: "", // no password set
            DB:       0,  // use default DB
        })

    }else {
        globals.logger.Info("MasterName hexmeet_redis in sentinel mode")
        conn = redis.NewFailoverClient(&redis.FailoverOptions{
            MasterName:    "hexmeet_redis",
            SentinelAddrs: []string{service_name + ":" + strconv.Itoa(port)},
            DB:       0,  // use default DB
        })
    }

    keyprefix := "beluga:"
    redis_con := &RedisConn{
        rdb:    conn,
        key_prefix: keyprefix,
    }

    go redis_con.run();
    return redis_con

}

func (conn *RedisConn ) run() {
    defer TryLogDump()

    pingTicker := time.NewTicker(defaultPingTimer)

    for {

        select {
        case <-pingTicker.C:
            _, err := conn.rdb.Ping(ctx).Result()
            if err != nil {
                globals.logger.Warnf("redis_conn: Ping %s", err.Error())
            }
            //fmt.Println(pong, err)
        }
    }
    
}

func (conn *RedisConn) ReadChatUser(usrId string)(string, error){

    key := conn.key_prefix + usrId
    val, err := conn.rdb.Get(ctx, key).Result()
   
    if err != nil {
        globals.logger.Warnf("redis_conn: ReadUser %s", err.Error())
    }
    return val, err
}

func (conn *RedisConn) ReadChatGroup(groupId string)(string, error){

    key := conn.key_prefix + groupId
    val, err := conn.rdb.Get(ctx, key).Result()
   
    if err != nil {
        globals.logger.Warnf("redis_conn: ReadChatGroup %s", err.Error())
    }
    return val, err
} 

func (conn *RedisConn) WriteChatUser (usrId string,value string) error {
    //log.Println("Write() in.")
    if nil == conn.rdb{
        //log.Printf("rdb is nil")
        return errors.New("redis connection not init")
    }
    key := conn.key_prefix + usrId
    //value := nodename + ":" + strconv.Itoa(port) + "##" + strconv.Itoa(topic_type)
    //log.Println("Write() in.key=",key)
    err := conn.rdb.Set(ctx, key, value, 0).Err()
    if err != nil {
        globals.logger.Warnf("redis_conn: WriteUser %s", err.Error())
    }
    return err
}

func (conn *RedisConn) WriteChatGroup (groupId string,value string) error {
    //log.Println("Write() in.")
    if nil == conn.rdb{
        //log.Printf("rdb is nil")
        return errors.New("redis connection not init")
    }
    key := conn.key_prefix + groupId
    //value := nodename + ":" + strconv.Itoa(port) + "##" + strconv.Itoa(topic_type)
    //log.Println("Write() in.key=",key)
    err := conn.rdb.Set(ctx, key, value, 0).Err()
    if err != nil {
        globals.logger.Warnf("redis_conn: WriteChatGroup %s", err.Error())
    }
    return err
}

func (conn *RedisConn) DeleteChatUser(usrId string)  error{
    key := conn.key_prefix + usrId
    return conn.rdb.Del(ctx,key).Err()
}

func (conn *RedisConn) DeleteChatGroup(groupId string)  error{
    key := conn.key_prefix + groupId
    return conn.rdb.Del(ctx,key).Err()
}


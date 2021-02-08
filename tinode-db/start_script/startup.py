import json
import jsoncomment
import os
import db

def main():
    with open("/usr/share/chat/tinode.conf",'r') as load_f:
        parser = jsoncomment.JsonComment(json)
        parsed_object = parser.loads(load_f.read())
        load_f.close()
        print(parsed_object['store_config']['adapters']['postgres']['database'])
        print(parsed_object['store_config']['adapters']['postgres']['psqlInfo'])

        database_name = parsed_object['store_config']['adapters']['postgres']['database']
        sqlInfo = parsed_object['store_config']['adapters']['postgres']['psqlInfo']

        # print(sqlInfo.partition(' '))
        #print (sqlInfo.split())
        username = None
        password = None
        host = None
        port = None

        for val_pair in sqlInfo.split():
            vals = val_pair.partition('=')
            if vals[0] == 'user':
                username = vals[2]
            elif vals[0] == 'password':
                password = vals[2]
            elif vals[0] == 'host':
                host = vals[2]
            elif vals[0] == 'port':
                port = vals[2]

        print(username, password, host,port)
        db_op = db.DBOperator(username,password,host,port,database_name)
        if db_op.check_db_exist():
            print('keep db data.')
        else:
            os.system('./tinode-db --reset --data /usr/share/chat/hexmeet_data.json')
'''
        if "node_name" in os.environ and "nat_ip" in os.environ:
            nodeName = os.environ["node_name"]
            host_ip = os.environ["host_ip"]
            nat_ip = host_ip
            if "nat_ip" in os.environ:
            	nat_ip = os.environ["nat_ip"]
            redis_name = "haishen_redis"
            if "redis_svc_name" in os.environ:
                redis_name =  os.environ["redis_svc_name"]
            redis_port = 9000
            if "redis_port" in os.environ:
                redis_port = os.environ["redis_port"]

            if len(nodeName) != 0:
                if "ID" in os.environ:
                    ID = os.environ["ID"]
                    if len(ID) != 0:
                        cmdLine = "nohup ./server -config=/usr/share/chat/tinode.conf -cluster_self=" + nodeName + " -cluster_port=9615" +" -cluster_nat_addr=" + nat_ip +" -cluster_host_addr=" + host_ip + " -redis_mode=1 -redis_addr="+redis_name + " -redis_port=" + str(redis_port) +" -static_data=/usr/share/chat/static"+" -worker_id="+ID+" -rpc_cert_path=/conf &"
                    else:
                        cmdLine = "nohup ./server -config=/usr/share/chat/tinode.conf -cluster_self=" + nodeName + " -cluster_port=9615" +" -cluster_nat_addr=" + nat_ip +" -cluster_host_addr=" + host_ip + " -redis_mode=1 -redis_addr="+redis_name + " -redis_port=" + str(redis_port) +" -static_data=/usr/share/chat/static"+" -rpc_cert_path=/conf &"
                else:
                    cmdLine = "nohup ./server -config=/usr/share/chat/tinode.conf -cluster_self=" + nodeName + " -cluster_port=9615" +" -cluster_nat_addr=" + nat_ip +" -cluster_host_addr=" + host_ip + " -redis_mode=1 -redis_addr="+redis_name + " -redis_port=" + str(redis_port) +" -static_data=/usr/share/chat/static"+" -rpc_cert_path=/conf &"
                print(cmdLine)
                os.system(cmdLine)
        else:
            os.system('nohup ./server -config=/usr/share/chat/tinode.conf -static_data=/usr/share/chat/static &')
            #monitor the cluster info, fetch the delta change
            #test
'''
if __name__ == '__main__':
    main()

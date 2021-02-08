
import psycopg2



class DBOperator:
    def __init__(self,username, pwd, host, port, db_name):
        self.username = username
        self.password = pwd
        self.host = host
        self.port = port
        self.db_name = db_name
    
    def drop_db(self):
        conn = None
        try:
            conn = psycopg2.connect(
               database='postgres', user=self.username, password=self.password, host=self.host, port=self.port
            )
        except:
            print('drop_db Database not connected.')
        
        if conn is not None:
            conn.autocommit = True
            cur = conn.cursor()
            q = "DROP database {}".format(self.db_name)
            cur.execute(q)
            conn.close()

    def check_db_exist(self):
        conn = None
        try:
            conn = psycopg2.connect(
               database='postgres', user=self.username, password=self.password, host=self.host, port=self.port
            )
        except:
            print('check_db_exist Database not connected.')
           
        if conn is not None: 
            conn.autocommit = True
        
            cur = conn.cursor()
        
            cur.execute("SELECT datname FROM pg_database;")
        
            list_database = cur.fetchall()
        
            database_name = self.db_name
            conn.close()
            if (database_name,) in list_database:
                print("'{}' Database already exist".format(database_name))
                return True
            else:
                print("'{}' Database not exist.".format(database_name))
        
        return False
        
        
    

    def get_table_foreign_contonstrain(self, tb_name):
        conn = None
        #establishing the connection
        try:
            conn = psycopg2.connect(
                database = self.db_name, user=self.username, password=self.password, host=self.host, port=self.port
            )
        except:
            print('get_table_foreign_contonstrain Database not connected.')
        
        if conn is not None:
            #Setting auto commit false
            conn.autocommit = True
            
            #Creating a cursor object using the cursor() method
            #cursor = conn.cursor()
            
            #cursor.execute("select * from subscriptions")
            #column_names = [desc[0] for desc in cursor.description]
            #print(column_names)
            
            q = """                              
            SELECT *
            FROM information_schema.table_constraints
            WHERE table_name = %s;
            """
            
            cur = conn.cursor()
            cur.execute(q, (tb_name,))  # (table_name,) passed as tuple
            result = cur.fetchall()
            #print(result)
            conn.close()
            for el in result:
                #print(el[2])
                if el[2] == 'subscriptions_userid_fkey':
                    return True
                
            return False;
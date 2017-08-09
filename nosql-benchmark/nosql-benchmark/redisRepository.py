from nosql_base import NosqlBase 
import redis

class RedisRepo():
    """Redis Repository"""
       
    def __init__(self, connection_name, database_name, port_no):
        self.dbname = database_name             
        self.client = redis.StrictRedis(host=connection_name, port=port_no, db=0)            
              
    def write(self, record, key, hash_name):
        self.client.hset(hash_name, key, record)
    
    def read(self, key, hash_name):
        return self.client.hget(hash_name, key)
        
    def query(self, **kwargs):
        return 

 
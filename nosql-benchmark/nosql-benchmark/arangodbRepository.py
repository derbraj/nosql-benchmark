from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *

class ArangoRepo():
    """ArangoDB Repository"""
    
    dbname = None
    username = None
    pw = None    
    connection = None 
    host = None 
    port = None 
    db = None
    collectionname = None
    collection = None

    def __init__(self, url, user_name, password, port_no, database):        
        self.username = user_name
        self.pw = password  
        self.port = port_no
        self.host = url + ':' + str(self.port)   
        self.dbname = database    
        self.connection = Connection(arangoURL=self.host, username=self.username, password=self.pw)

    def create_db(self):                 
        if not self.connection.hasDatabase(name=self.dbname):
           self.connection.createDatabase(name=self.dbname)         
        self.db = self.connection[self.dbname]    
    
    def delete_db(self):                 
        if self.connection.hasDatabase(name=self.dbname):
           x = 'del' 

    def create_collection(self, collection_name):  
        self.collectionname = collection_name
        self.collection = self.db.createCollection(name=self.collectionname)

    def open_collection(self, collection_name):
        self.collectionname = collection_name
        self.collection = self.db[self.collectionname]
        x = 10

    def write(self, key, record):
        if key != None:
           record["_key"] = str(key)            
        bind = {"doc": record}
        aql = "INSERT @doc INTO " + self.collectionname + " LET newDoc = NEW RETURN newDoc"
        queryResult =  self.db.AQLQuery(aql, bindVars=bind)

    def read(self, key):
        return self.collection[key]
  

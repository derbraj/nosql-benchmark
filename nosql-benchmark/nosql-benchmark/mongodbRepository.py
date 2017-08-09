from pymongo import MongoClient

class MongoRepo():
    """MongoDB"""
       
    def __init__(self, connection_name, database_name, port_no):
    #     MongoClient.connect('mongodb://' + host + ':27017/pokec', {
    #  server: {
    #    auto_reconnect: true,
    #    poolSize: 25,
    #    socketOptions: {keepAlive: 1}
    #  }
    #}
        self.dbname = database_name
        self.client = MongoClient(host=connection_name, port=port_no)
        self.collection = None
        self.collection_name = None

    def create_db(self):
        self.db = self.client[self.dbname]

    def delete_db(self):
        self.client.drop_database(self.dbname)

    def create_collection(self, collectionname):
        self.collection_name = collectionname
        self.collection = self.db.create_collection(self.collection_name)

    def open_collection(self, collectionname):
        self.collection_name = collectionname
        self.collection = self.db.get_collection(name=self.collection_name)               

    def write(self, key, record):
        if key != None:
           record["_id"] = key   
        self.collection.insert(record)
    
    def read(self, key):
        result = self.db[self.collection_name].find_one({"_id": key})
        return result

    def query(self, **kwargs):
        return list(self.collection.find(kwargs))

    def get_all_ids(self):
        return self.collection.distinct('_id')

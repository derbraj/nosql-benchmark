class NosqlBase(object):
    """Base class where the basic nosql database operation contracts are defined."""

    dbname = None   
    connection = None 
    port = None
    
    def __init__(self, connection_name, database_name, port_no):
        self.connection = connection_name
        self.dbname = database_name
        self.port = port_no
        self.create_db()

    def create_db(self):
        raise NotImplementedError("Must implement a 'create_db' method.")

    def delete_db(self):
        raise NotImplementedError("Must implement a 'create_db' method.")
          
    def create(self, record):
        raise NotImplementedError("Must implement a 'create' method.")
    
    def read(self, key):
        raise NotImplementedError("Must implement a 'read' method.")
    
    def query(self, **kwargs):
        raise NotImplementedError("Must implement a 'query' method.")
        
    def __del__(self):
        self.delete_db()
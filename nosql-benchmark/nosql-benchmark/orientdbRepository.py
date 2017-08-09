import pyorient
import pyorient.ogm

class OrientRepo():
    """OrientDB Repository"""
    
    dbname = None
    username = None
    pw = None    
    client = None 
    session_id = None
    host = None 
    port = None 
    db = None
    cluster_id = None
    classname = None

    def __init__(self, host_name, user_name, password, port_no):        
        self.username = user_name
        self.pw = password  
        self.port = port_no
        self.host = host_name
        self.client = pyorient.OrientDB(self.host, self.port)
        self.session_id = self.client.connect(self.username, self.pw)

    def create_db(self, database, db_type):                 
        if not self.client.db_exists(database, pyorient.STORAGE_TYPE_MEMORY):
           if db_type == 'GRAPH':
              self.client.db_create(database, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY) 
           else:
              self.client.db_create(database, pyorient.DB_TYPE_DOCUMENT, pyorient.STORAGE_TYPE_MEMORY)  
        self.client.db_open(database, self.username, self.pw)
    
    def close_db(self):
        self.client.db_close()

    def create_class(self, class_name):  
        self.classname = class_name
        clust_id = self.get_cluster_id(self.classname)
        if clust_id == -1:
           clust_id = self.client.command('create class ' + self.classname)
           clust_id = self.get_cluster_id(self.classname)
        self.cluster_id = clust_id

    def write(self, key, record):
        record["id"] = str(key)    
        rec = {}
        rec['@' + self.classname] = record       
        rec_position = self.client.record_create(int(self.cluster_id), rec)
       

    def read_by_id(self, id):
        return self.client.query('select from ' + self.classname + ' where @rid = ' + str(self.cluster_id) + ':' + str(id))

    def read(self, key):
        return self.client.query('select from ' + self.classname + ' where id = ' + str(key))


    def get_cluster_id(self, class_name):  
        clus_id = -1
        for cluster in self.client.clusters:
            if cluster.name == class_name:
               clus_id = cluster.id
               break
        return clus_id
      
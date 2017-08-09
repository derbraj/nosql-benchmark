from template import Template
from random import randint
from mongodbRepository import MongoRepo
from redisRepository import RedisRepo
from arangodbRepository import ArangoRepo
from orientdbRepository import OrientRepo
import uuid
import datetime
import time
import csv


def main():  
  #benchmark_databases()
  #database_to_test = 'mongodb'
  #port = 32769
  
  #database_to_test = 'arangodb'
  #port = 32771
  #host_name = 'localhost'
  
  #database_to_test = 'orientdb'
  #port = 32777
  #host_name = 'localhost'

  target_count = 100000
  user_name='root'
  password='rootpwd'
  benchmark_document_single_write(database_to_test, host_name, port, target_count, user_name, password)
  #benchmark_document_single_read(database_to_test, host_name, port, target_count, user_name, password)

def get_product_catelog_document():
    file_name = "productCatalog.json"    
    item_id = randint(10000, 9999999) 
    parent_item_id = randint(10000, 9999999) 
    document = Template("data").get_product_template(file_name, item_id, parent_item_id) 
    return document

def get_shopping_cart_data():
    file_name = "shoppingCart.json"    
    cart_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    document = Template("data").get_cart_template(file_name, cart_id, user_id) 
    return document

def report_result(database_name, test_name, times, time_started, time_ended, transactions, duration):
    filename = test_name + '_' + database_name
    with open(filename, "wb") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Test Name", "DB Name", "Transactions", "Duration", "Start Time", "Stop Time", "AVG"])
        writer.writerow([test_name, database_name, transactions, duration,  time_started, time_ended, (duration/transactions)])

    filename = test_name + '_' + database_name + '_details'
    with open(filename, "wb") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Test Name", "DB Name", "Duration"])        
        for item in times:
            row = [test_name]
            row += [database_name]
            row += [item]
            writer.writerow(row)

def benchmark_document_single_write(database_to_test, host_name, port, target_count, user_name, password):   
    database_name = database_to_test + '_test'
    collection_name = database_to_test + '_test_collection'
   
    overall_start = time.time()
    overall_stop = time.time()
    times = []

    #create document list
    productDocuments = [get_product_catelog_document()]
    for index in range(target_count - 1):
        productDocuments.append(get_product_catelog_document())  
        
    if database_to_test == 'mongodb':
       mongoClient = MongoRepo(host_name, database_name, port)
       try:
            mongoClient.delete_db()
       except:
            print "Database does not exists."
       mongoClient.create_db()
       mongoClient.create_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for doc in productDocuments:
           key = 500000 + count
           start = time.time()
           mongoClient.write(key, doc)
           stop = time.time()
           times.append(stop - start)
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'arangodb':   
       url = 'http://' + host_name
       arangoClient = ArangoRepo(url=url, user_name=user_name, password=password, port_no=port, database=database_name)
       try:
            arangoClient.delete_db()
       except:
            print "Database does not exists."
       arangoClient.create_db()
       arangoClient.create_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for doc in productDocuments:
           key = str(500000 + count)
           start = time.time()
           arangoClient.write(key, doc)
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'orientdb':
       orientClient = OrientRepo(host_name=host_name, user_name=user_name, password=password, port_no=port)
       #try:
       #     orientClient.delete_db()
       #except:
       #     print "Database does not exists."
       orientClient.create_db(database_name, 'DOCUMENT')
       orientClient.create_class(class_name=collection_name)

       overall_start = time.time() 
       count = 0
       for doc in productDocuments:
           key = str(500000 + count)
           start = time.time()
           orientClient.write(key, doc)
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    else:
       print "not a valid database for this test."
       
    duration = overall_stop - overall_start    
    report_result(database_to_test, 'benchmark_document_single_write', times, overall_start, overall_stop, target_count, duration) 

def benchmark_document_single_read(database_to_test, host_name, port, target_count, user_name, password):   
    database_name = database_to_test + '_test'
    collection_name = database_to_test + '_test_collection'
 
    overall_start = time.time()
    overall_stop = time.time()
    times = []   
    
    #benchmark database
    if database_to_test == 'mongodb':
       mongoClient = MongoRepo(host_name, database_name, port)
       
       mongoClient.create_db()
       mongoClient.open_collection(collection_name)

       overall_start = time.time()
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           doc = mongoClient.read(key)
           stop = time.time()
           times.append(stop - start)
           if doc == None:
              raise LookupError("Not found")
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'arangodb':   
       url = 'http://' + host_name
       arangoClient = ArangoRepo(url=url, user_name=user_name, password=password, port_no=port, database=database_name)
       try:
            arangoClient.delete_db()
       except:
            print "Database does not exists."
       arangoClient.create_db()
       arangoClient.open_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           doc = arangoClient.read(key)
           if doc == None:
              raise LookupError("Not found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'orientdb':
       orientClient = OrientRepo(host_name=host_name, user_name=user_name, password=password, port_no=port)
       orientClient.create_db(database_name, 'DOCUMENT')
       orientClient.create_class(class_name=collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           d = orientClient.read_by_id(count)
           if d == None:
               raise LookupError("Not Found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1

       overall_stop = time.time()

    else:
       print "not a valid database for this test."
       
    duration = overall_stop - overall_start    
    report_result(database_to_test, 'benchmark_document_single_read', times, overall_start, overall_stop, target_count, duration)

def benchmark_KeyValue_single_write(database_to_test, host_name, port, target_count, user_name, password):   
    database_name = database_to_test + '_test'
    collection_name = database_to_test + '_test_collection'
   
    overall_start = time.time()
    overall_stop = time.time()
    times = []

    #Prepare document
    carts = [get_shopping_cart_data()]
    for index in range(target_count - 1):
        carts.append(get_shopping_cart_data())  
        
    if database_to_test == 'redis':
       client = RedisRepo(host_name, database_name, port)
       hash_name = 'cart_data'

       overall_start = time.time() 
       count = 0
       for doc in carts:
           key = 500000 + count
           start = time.time()
           client.write(doc, key, hash_name)
           stop = time.time()
           times.append(stop - start)
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'arangodb':   
       url = 'http://' + host_name
       arangoClient = ArangoRepo(url=url, user_name=user_name, password=password, port_no=port, database=database_name)
       try:
            arangoClient.delete_db()
       except:
            print "Database does not exists."
       arangoClient.create_db()
       arangoClient.create_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for doc in carts:
           key = str(500000 + count)
           start = time.time()
           arangoClient.write(key, doc)
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'orientdb':
       orientClient = OrientRepo(host_name=host_name, user_name=user_name, password=password, port_no=port)
       #try:
       #     orientClient.delete_db()
       #except:
       #     print "Database does not exists."
       orientClient.create_db(database_name, 'DOCUMENT')
       orientClient.create_class(class_name=collection_name)

       overall_start = time.time() 
       count = 0
       for doc in carts:
           key = str(500000 + count)
           start = time.time()
           orientClient.write(key, doc)
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    else:
       print "not a valid database for this test."
       
    duration = overall_stop - overall_start    
    report_result(database_to_test, 'benchmark_document_single_write', times, overall_start, overall_stop, target_count, duration) 

    def benchmark_document_single_read(database_to_test, host_name, port, target_count, user_name, password):   
    database_name = database_to_test + '_test'
    collection_name = database_to_test + '_test_collection'
 
    overall_start = time.time()
    overall_stop = time.time()
    times = []   
    
    #benchmark database
    if database_to_test == 'mongodb':
       mongoClient = MongoRepo(host_name, database_name, port)
       
       mongoClient.create_db()
       mongoClient.open_collection(collection_name)

       overall_start = time.time()
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           doc = mongoClient.read(key)
           stop = time.time()
           times.append(stop - start)
           if doc == None:
              raise LookupError("Not found")
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'arangodb':   
       url = 'http://' + host_name
       arangoClient = ArangoRepo(url=url, user_name=user_name, password=password, port_no=port, database=database_name)
       try:
            arangoClient.delete_db()
       except:
            print "Database does not exists."
       arangoClient.create_db()
       arangoClient.open_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           doc = arangoClient.read(key)
           if doc == None:
              raise LookupError("Not found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'orientdb':
       orientClient = OrientRepo(host_name=host_name, user_name=user_name, password=password, port_no=port)
       orientClient.create_db(database_name, 'DOCUMENT')
       orientClient.create_class(class_name=collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           d = orientClient.read_by_id(count)
           if d == None:
               raise LookupError("Not Found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1

       overall_stop = time.time()

    else:
       print "not a valid database for this test."
       
    duration = overall_stop - overall_start    
    report_result(database_to_test, 'benchmark_document_single_read', times, overall_start, overall_stop, target_count, duration)

def benchmark_KeyValue_single_read(database_to_test, host_name, port, target_count, user_name, password):   
    database_name = database_to_test + '_test'
    collection_name = database_to_test + '_test_collection'
 
    overall_start = time.time()
    overall_stop = time.time()
    times = []   
    
    if database_to_test == 'redis':
       client = RedisRepo(host_name, database_name, port)
       hash_name = 'cart_data'

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           data = client.read(key, hash_name)
           stop = time.time()
           times.append(stop - start)
           count = count + 1
       overall_stop = time.time()
          
    elif database_to_test == 'arangodb':   
       url = 'http://' + host_name
       arangoClient = ArangoRepo(url=url, user_name=user_name, password=password, port_no=port, database=database_name)
       try:
            arangoClient.delete_db()
       except:
            print "Database does not exists."
       arangoClient.create_db()
       arangoClient.open_collection(collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           doc = arangoClient.read(key)
           if doc == None:
              raise LookupError("Not found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1
       overall_stop = time.time()

    elif database_to_test == 'orientdb':
       orientClient = OrientRepo(host_name=host_name, user_name=user_name, password=password, port_no=port)
       orientClient.create_db(database_name, 'DOCUMENT')
       orientClient.create_class(class_name=collection_name)

       overall_start = time.time() 
       count = 0
       for key in range(500000, 500000 + target_count):
           start = time.time()
           d = orientClient.read_by_id(count)
           if d == None:
               raise LookupError("Not Found")
           stop = time.time()
           times.append(stop - start) 
           count = count + 1

       overall_stop = time.time()

    else:
       print "not a valid database for this test."
       
    duration = overall_stop - overall_start    
    report_result(database_to_test, 'benchmark_document_single_read', times, overall_start, overall_stop, target_count, duration)

#single write - time to load 100K vs time to load 500K
#
def benchmark_databases():
    productDocument = get_product_catelog_document()  
    #documentClient = MongoRepo('localhost:32771', 'test11')
    #documentClient.create_db()
    #documentClient.create(doc)
    #key_list = con.get_all_ids()  
    #cartDocument = get_shopping_cart_document()
    #keyValClient  = RedisRepo('localhost', '', 32773)
    #keyValClient.create(cartDocument, '0001', 'test')
    #result = keyValClient.read('0001', 'test')    
    #arangoDocumentClient = ArangoRepo(url='http://localhost',user_name='root', password='1Tq9uW9ZvZrK5hze',port_no='32774',database='test001')
    #arangoDocumentClient.create_db()
    #arangoDocumentClient.create_collection('Perfcol')
    #arangoDocumentClient.create('0001', productDocument, 'Perfcol
    #orientDocumentClient = OrientRepo(host_name='localhost', user_name='root', password='rootpwd', port_no=32775)
    #orientDocumentClient.create_db('test003', 'DOCUMENT')
    #orientDocumentClient.create_class(class_name='testclass')
    #orientDocumentClient.create('0001', productDocument)
    
    print 'OK'

if __name__ == '__main__':
  main()
#! python3
# restaurant.py - 1.Writing raw data into MongoDB (1 - 3)
#                 2.  

import csv, os, pymongo, pprint, json


#reads in csv file and gives back a list
def get_data(adress, my_delimiter):
    data = []
    with open(adress, 'r') as csvFile:
        reader = csv.reader(csvFile, delimiter = my_delimiter)
        for row in reader:
            dataList.append(row)
    return data
    
#gives back the mongodb-connection to a given database
def get_db(db_name):
    from pymongo import MongoClient
    
    client = pymongo.MongoClient("mongodb://Alex:geheim21@cluster0-shard-00-00-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-01-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-02-ufyat.gcp.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&w=majority")
    db = client[db_name]    
    return db

    
#stores given dataList in a collection in the db
def store_in_database(db, collection, dataList):
    #if collection already exists it is deleted
    db[collection].drop()
    my_collection = db[collection]
    print("Inserting into " + collection + "...")
    
    #"id" becomes "_id" 
    dataList[0][0] = "_id"
    dataIterator = iter(dataList)
    
    #reading the first row (contains header data)
    header = next(dataIterator)
    print("Data Header: " + str(header))
    insert_count = 0
    
    #writing every row of the list into the MongoDB
    for row in dataIterator:
        #building a dictionary out of the header and the row
        document = (dict(zip(header, row)))
        #storing the dictionary in the DB
        data_id = my_collection.insert_one(document).inserted_id
        insert_count = insert_count + 1
        
    print("Inserted " + str(insert_count) + " documents")
    
 
 
#-----MAIN-METHODE-----    
data = get_data('restaurant_data/restaurants.tsv', '\t')
db = get_db("dmdb_project")
store_in_database(db, "restaurant_data", data)




# data_handler.py - Getting input from csv, writing data into MongoDB, getting data from MongoDB 

import csv, os, pymongo, pprint, json


#reads in csv file and gives it back as a list
def get_data(adress, my_delimiter):
    data = []
    with open(adress, 'r') as csvFile:
        reader = csv.reader(csvFile, delimiter = my_delimiter)
        for row in reader:
            data.append(row)
    print("Got " + str(len(data)) + " lines of data from " + adress)
    return data
    
#gives back the mongodb-connection to a given database
def get_db(db_name, collection_name):
    from pymongo import MongoClient
    
    client = pymongo.MongoClient("mongodb://Alex:geheim21@cluster0-shard-00-00-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-01-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-02-ufyat.gcp.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&w=majority")
    db = client[db_name]    
    db_col = db[collection_name]
    print("Got DB-Connection " + db_col.full_name)
    return db_col
    
#stores given data-list in a given collection in the db
def store_in_database(db_collection, data):
    #if "id" exists, it becomes "_id" 
    if(data[0][0] == "id"):
        data[0][0] = "_id"
    dataIterator = iter(data)
    
    #reading the first row (contains header data)
    header = next(dataIterator)
    insert_count = 0
    
    #writing every row of the list into the MongoDB
    for row in dataIterator:
        #building a dictionary out of the header and the row
        new_document = (dict(zip(header, row)))
        #looking if the document already exists
        db_document = db_collection.find_one(new_document)
        if(db_document is None):
            #storing the dictionary in the DB
            db_collection.insert_one(new_document)
            insert_count = insert_count + 1
        
    print("Inserted " + str(insert_count) + " documents into " + db_collection.full_name)
    
#updates the given documents in the given connection using the Id field
def update_database(db_collection, data):
    update_count = 0
    for document in data:
        db_collection.replace_one({"_id": document["_id"]},document)
        update_count = update_count + 1
    print("Updated " + str(update_count) + " documents in " + db_collection.full_name)    
# data_handler.py - Getting input from csv, writing data into MongoDB, getting data from MongoDB 

import csv, os, pymongo, json, re, sys, time

def my_print(message):
    #print to console
    print(message, flush=True)
    
    #print to .log file
    message = message.split("\n")
    for line in message:
        timestamp = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime()) 
        line = timestamp + ": " + line
        print(line, file=open("find_duplicates.log", "a"), flush=True)

#reads in csv file and gives it back as a list
def get_data(adress, my_delimiter):
    data = []
    with open(adress, 'r') as csvFile:
        reader = csv.reader(csvFile, delimiter = my_delimiter)
        for row in reader:
            data.append(row)
    my_print("Got " + str(len(data)) + " lines of data from " + adress)
    return data
    
#gives back the mongodb-connection to a given database
def get_db(db_name, collection_name):
    from pymongo import MongoClient
    
    client = pymongo.MongoClient("mongodb://Alex:geheim21@cluster0-shard-00-00-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-01-ufyat.gcp.mongodb.net:27017,cluster0-shard-00-02-ufyat.gcp.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&w=majority")
    db = client[db_name]    
    db_col = db[collection_name]
    my_print("Got DB-Connection " + db_col.full_name)
    return db_col
    
#stores given data-list in a given collection in the db
def store_in_database(db_collection, data):
    db_collection.drop()
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
        #storing the dictionary in the DB
        db_collection.insert_one(new_document)
        insert_count = insert_count + 1
        
    my_print("Inserted " + str(insert_count) + " documents into " + db_collection.full_name + "\n")
    
#updates the given documents in the given connection using the Id field
def update_database(db_collection, data):
    update_count = 0
    for document in data:
        db_collection.replace_one({"_id": document["_id"]},document)
        update_count = update_count + 1
    my_print("Updated " + str(update_count) + " documents in " + db_collection.full_name)    

#cleans the field phone
def clean_phone(db_collection):
    #replaces the "/" with "-"
    pipeline = [
                {'$match': {'phone': {'$regex': '[0-9]{3}/[0-9]{3}-[0-9]{4}'}}}
               ]
    data = list(db_collection.aggregate(pipeline))
    for row in data:
        phone = row["phone"].split("/")
        row["phone"] = phone[0] + "-" + phone[1]
    update_database(db_collection, data)
    my_print("Cleaned the field phone\n")
    
#cleans the field city
def clean_city(db_collection):
    #if the city is "la" it becomes "los angeles"
    pipeline = [{'$match': {'city': 'la'}}]
    data = list(db_collection.aggregate(pipeline))
    for row in data:
        row["city"] = "los angeles"
    update_database(db_collection,data)
    
    #if the city is "new york city" it becomes "new york"
    pipeline = [{'$match': {'city': 'new york city'}}]
    data = list(db_collection.aggregate(pipeline))
    for row in data:
        row["city"] = "new york"
    update_database(db_collection,data)
    my_print("Cleaned the field city\n")

#cleans the field address
def clean_address(db_collection):
    #separates the address string into postcode and streetname and buildes a key-value for both
    data = list(db_collection.find())
    for row in data:
        address = str(row["address"])
        postcode = re.findall("^\d+", address)   #gives back the first digits as postcode
        if postcode != []:
            postcode = postcode[0]
        else:
            postcode = "0"
        streetname = address[len(postcode):].lstrip()
        if re.findall("^[wesn][.]\s", streetname):
            streetname = streetname[3:]
        streetname = streetname[ :5].strip()
        
        address_key = str(postcode) + " " + str(streetname)
        #address_tupel = [{"postcode": postcode}, {"streetname": streetname}]
        row["address"] = address_key
        
    update_database(db_collection, data)    
    my_print("Cleaned the field address\n")

#cleans the field type
def clean_type(db_collection):
    pipeline = [{'$match': {'type': {'$regex': '[(]'}}}]
    data = list(db_collection.aggregate(pipeline))
    for row in data:
        row["type"] = row["type"].split()[0].strip()
    
    update_database(db_collection, data)    
    my_print("Cleaned the field type\n")
    
#help function for find_duplicates. Returns a list with the names of multiple entries of the passed field name
def find_multiple_entries(db_collection, field_name):
    pipeline = [
                    {'$group': {'_id': '$' + field_name, 
                                'count': {'$sum': 1}}}, 
                    {'$match': {'count': {'$gt': 1}}}, 
                    {'$sort': {'count': -1, 
                                '_id': 1}}, 
                    {'$project': {'_id': 0, 
                                  field_name: '$_id', 
                                  'count': 1}}
                   ]
            
    return list(db_collection.aggregate(pipeline))

#return duplicates to the given field names
def find_duplicates(db_collection, search_list):
    duplicates = []
    
    for i in range(len(search_list)):  #iterates through search list (to get fieldnames)
        duplicates_cache = []
        grouped_duplicates = find_multiple_entries(db_collection, search_list[i])   #finds duplicates for specified fieldname (from search_list)
        for doc in grouped_duplicates:  #iterates through duplicates-id-list
            pipeline = [{"$match": {search_list[i]: doc[search_list[i]]}}]  #gets the documents matching the duplicate-fieldname value
            docs = list(db_collection.aggregate(pipeline))
            for j in range(len(docs) - 1):   #iterates through those documents
                id_1 = docs[j]["_id"]
                id_2 = docs[j + 1]["_id"]
                if int(float(id_1)) < int(float(id_2)):  #stores the ids in a list of dictionaries (lower value first)
                    duplicates_cache.append({"id_1": id_1, "id_2": id_2})
                elif int(float(id_1)) > int(float(id_2)):
                    duplicates_cache.append({"id_1": id_2, "id_2": id_1})
        #checks if these duplicates have already been found, and if not saves them in the list 
        if i == 0:  
            for doc in duplicates_cache:            
                duplicates.append(doc)   
        elif i > 0:
            remove = []
            for k in range(len(duplicates)):
                if duplicates[k] not in duplicates_cache:
                    remove.append(k)    
            remove.sort(reverse=True)
            for l in remove:
                duplicates.pop(l)
                
    my_print("Found " + str(len(duplicates)) + " duplicates for input " + str(search_list))
    return duplicates
        
#stores the duplicate-pairs unique
def safe_store_duplicates(db_collection, duplicates):
    #gets the existing duplicates from the database
    pipeline = [{'$project': {'_id': 0, 
                              'id_1': 1, 
                              'id_2': 1}}
               ]
    old_duplicates = list(db_collection.aggregate(pipeline))
    db_collection.drop()
    remove = []
    for i in range(len(duplicates)):   #iterates through given duplicates-list
        if duplicates[i] in old_duplicates:   #if duplicate already exists it will be removed from the duplicates-list
            remove.append(i)    
    remove.sort(reverse=True)
    for j in remove:
        duplicates.pop(j)
    
    #the remaining list is written back into the database together with the old duplicates
    for document in duplicates:   
        old_duplicates.append(document)
    for document in old_duplicates:
        db_collection.insert_one(document)
         
#compares the given duplicates with my duplicates
def compare_duplicates(db_collection, db_given_duplicates, db_my_duplicates):
    
    correctly_recognized_duplicates = []
    incorrectly_recognized_duplicates = []
    unrecognised_duplicates = []
    
    #gets the duplicates from the database
    pipeline = [{'$project': {'_id': 0, 
                              'id_1': 1, 
                              'id_2': 1}}
               ]
    my_dupl = list(db_my_duplicates.aggregate(pipeline))   #getting my duplicates
    given_dupl = list(db_given_duplicates.aggregate(pipeline))   #getting the given duplicates
    my_print("\nrecognized duplicates: " + str(len(my_dupl)))
    
    #writting the duplicates in the corresponding list
    for my_doc in my_dupl:
        if my_doc in given_dupl:
            correctly_recognized_duplicates.append(my_doc)
        elif my_doc not in given_dupl:
            incorrectly_recognized_duplicates.append(my_doc)
    
    for giv_doc in given_dupl:
        if giv_doc not in my_dupl:
            unrecognised_duplicates.append(giv_doc)
    
    #calculation of the different measurements
    data_sets = len(list(db_collection.find()))
    posible_pairs = 0.5 * (data_sets * data_sets - data_sets)
    len_md = len(my_dupl)
    p = len(given_dupl)
    n = posible_pairs - p
    tp = len(correctly_recognized_duplicates)
    fp = len(incorrectly_recognized_duplicates)
    fn = len(unrecognised_duplicates)
    tn = n - fp
    precision = tp / len_md * 100
    recall = tp / p * 100
    f_score = 2 * (precision * recall) / (precision + recall)
    accuracy = ((tp + tn)/(tp + tn + fp + fn)) * 100
    balanced_accuracy = (((tp / p) + (tn / (tn + fp))) / 2) * 100
    
    my_print("\ncorrectly recognized duplicates: " + str(tp))
    my_print("\nincorrectly_recognized_duplicates: " + str(fp))
    for doc in incorrectly_recognized_duplicates:
        my_print(str(doc))
    my_print("\nunrecognised_duplicates: (length: " + str(fn) + ")")
    for doc in unrecognised_duplicates:
        my_print(str(doc))
        
    my_print("\n")
    my_print("========================================")
    my_print("Precision: " + str(round(precision, 2)) + "% " + rate_result(precision))
    my_print("Recall: " + str(round(recall, 2)) + "% " + rate_result(recall))
    my_print("F-Score: " + str(round(f_score, 2)) + "% " + rate_result(f_score))
    #my_print("Accuracy: " + str(round(accuracy, 10)) + "% ")
    my_print("Balanced Accuracy: " + str(round(balanced_accuracy, 2)) + "% " + rate_result(balanced_accuracy))
    my_print("========================================")
    my_print("\n")
    my_print("\n")
    
    
def rate_result(value):
    if value >= 97.5:
        return "(very high)"
    if value >= 95:
        return "(high)"
    if value >= 92.5:
        return "(average)"
    if value >= 90:
        return "(low)"
    else:
        return "(very low)"
    
    
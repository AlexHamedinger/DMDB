#! python3
# main.py -----MAIN-METHODE-----    

import modules, time

start = time.time()
#####LOAD RAW DATA FROM RESTAURANT.TSV INTO MONGO-DB AND GET DB-CONNECTIONS#####
#data = modules.data_handler.get_data('restaurant_data/restaurants.tsv', '\t')
#given_duplicates = modules.data_handler.get_data('restaurant_data/restaurants_DPL.tsv', '\t')
db_collection = modules.data_handler.get_db("dmdb_project", "restaurant_data")
db_given_duplicates = modules.data_handler.get_db("dmdb_project", "restaurant_given_duplicates")
db_my_duplicates = modules.data_handler.get_db("dmdb_project", "restaurant_my_duplicates")
#modules.data_handler.store_in_database(db_collection, data)
#modules.data_handler.store_in_database(db_given_duplicates, given_duplicates)


#####BEAUTIFY THE FIELDS PHONE AND CITY#####
#modules.data_handler.clean_phone(db_collection)
#modules.data_handler.clean_city(db_collection)
#modules.data_handler.clean_address(db_collection)
#TODO: modules.data_handler.clean_name(db_collection)

#####FIND DUPLICATES#####
#db_my_duplicates.drop()
#duplicates = modules.data_handler.find_duplicates(db_collection, ["name", "phone"])
#modules.data_handler.safe_store_duplicates(db_my_duplicates, duplicates)
#duplicates = modules.data_handler.find_duplicates(db_collection, ["phone", "address"])
#modules.data_handler.safe_store_duplicates(db_my_duplicates, duplicates)
#duplicates = modules.data_handler.find_duplicates(db_collection, ["phone", "city"])
#modules.data_handler.safe_store_duplicates(db_my_duplicates, duplicates)
#duplicates = modules.data_handler.find_duplicates(db_collection, ["address", "city"])
#modules.data_handler.safe_store_duplicates(db_my_duplicates, duplicates)

#####COMPARE TO GIVEN DUPLICATES#####
print("\n\n########## RESULTS ##########")
modules.data_handler.compare_duplicates(db_given_duplicates, db_my_duplicates)

print("\nProcess terminated after " + str(time.time() - start) + " seconds...")
#! python3
# main.py -----MAIN-METHODE-----    

import modules


#####LOAD RAW DATA FROM RESTAURANT.TSV INTO MONGO-DB#####
data = modules.data_handler.get_data('restaurant_data/restaurants.tsv', '\t')
db_col = modules.data_handler.get_db("dmdb_project", "restaurant_data")
db_dup = modules.data_handler.get_db("dmdb_project", "restaurant_duplicates")
modules.data_handler.store_in_database(db_col, data)


#####BEAUTIFY THE FIELDS PHONE AND CITY#####
modules.data_handler.clean_phone(db_col)
modules.data_handler.clean_city(db_col)
modules.data_handler.clean_address(db_col)

#####FIND DUPLICATES#####
#db_dup.drop()
duplicates = modules.data_handler.find_duplicates(db_col, ["name", "phone"])
modules.data_handler.safe_store_duplicates(db_dup, duplicates)
duplicates = modules.data_handler.find_duplicates(db_col, ["phone", "address"])
modules.data_handler.safe_store_duplicates(db_dup, duplicates)
duplicates = modules.data_handler.find_duplicates(db_col, ["address", "city"])
modules.data_handler.safe_store_duplicates(db_dup, duplicates)




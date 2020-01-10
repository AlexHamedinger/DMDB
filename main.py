#! python3
# main.py -----MAIN-METHODE-----    

import modules


#####LOAD RAW DATA FROM RESTAURANT.TSV INTO MONGO-DB#####
data = modules.data_handler.get_data('restaurant_data/restaurants.tsv', '\t')
db_col = modules.data_handler.get_db("dmdb_project", "restaurant_data")
modules.data_handler.store_in_database(db_col, data)

#####BEAUTIFY THE FIELDS PHONE AND CITY#####
phonenumbers_data = list(db_col.aggregate(modules.pipeline.c()))
for row in phonenumbers_data:
    phone = row["phone"].split("/")
    row["phone"] = phone[0] + "-" + phone[1]
modules.data_handler.update_database(db_col, phonenumbers_data)

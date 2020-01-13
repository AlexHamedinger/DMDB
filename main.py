#! python3
# main.py -----MAIN-METHODE-----    

import modules, time

start = time.time()

###################################* STEP 1 *####################################
#####LOAD RAW DATA FROM RESTAURANT_DATA INTO MONGO-DB AND GET DB-CONNECTIONS#####
#get data from restaurants.tsv and stores it in the database
#data = modules.data_handler.get_data('restaurant_data/restaurants.tsv', '\t')  
db_collection = modules.data_handler.get_db("dmdb_project", "restaurant_data") 
#modules.data_handler.store_in_database(db_collection, data)   

#get data from restaurants_DUP.tsv and stores it in the database
#given_duplicates = modules.data_handler.get_data('restaurant_data/restaurants_DPL.tsv', '\t')   
db_given_duplicates = modules.data_handler.get_db("dmdb_project", "restaurant_given_duplicates")
#modules.data_handler.store_in_database(db_given_duplicates, given_duplicates)

#get database collection where the found duplicates will be stored
db_my_duplicates = modules.data_handler.get_db("dmdb_project", "restaurant_my_duplicates")


###################* STEP 2 *####################
##### CLEAN THE FIELDS PHONE, NAME AND CITY #####
#modules.data_handler.clean_phone(db_collection)
#modules.data_handler.clean_city(db_collection)
#modules.data_handler.clean_address(db_collection)
#modules.data_handler.clean_type(db_collection)

##############* STEP 3 *##############
#####FIND DUPLICATES AND EVALUATE#####
#The following lists contain lists with combinations of field names which will be checked for duplicates
check_field_combination_0 = [["name"],
                              ["address", "city"], 
                              ["phone"]]
check_field_combination_1 = [["name", "address"],
                              ["name", "city"],
                              ["name", "phone"],
                              ["address", "city"],
                              ["address", "phone"],
                              ["city", "phone"]]
check_field_combination_2 = [["name", "address"],
                              ["name", "city"],
                              ["name", "phone"],
                              ["address", "phone"],
                              ["city", "phone"]]
check_field_combination_3 = [["name", "address"],
                              ["name", "city"],
                              ["name", "phone"],
                              ["address", "phone"]]
check_field_combination_4 = [["name", "address"],
                              ["name", "city"],
                              ["name", "phone"],
                              ["city", "phone"]]
check_field_combination_5 = [["name", "address", "city"],
                             ["name", "address", "phone"],
                             ["name", "city", "phone"],
                             ["address", "city", "phone"]]
check_field_combination_6 = [["name", "address", "city"],
                             ["name", "address", "phone"],
                             ["name", "address", "type"],
                             ["name", "city", "phone"],
                             ["name", "city", "type"],
                             ["name", "phone", "type"],
                             ["address", "city", "phone"],
                             ["address", "city", "type"],
                             ["address", "phone", "type"],
                             ["city", "phone", "type"]]


                                
all_field_combinations = []
all_field_combinations.append(check_field_combination_0)
all_field_combinations.append(check_field_combination_1)
all_field_combinations.append(check_field_combination_2)
all_field_combinations.append(check_field_combination_3)
all_field_combinations.append(check_field_combination_4)
all_field_combinations.append(check_field_combination_5)
all_field_combinations.append(check_field_combination_6)



#Loops through the field name combinations and evaluates them
for check_field_combination in all_field_combinations:
    print("\n")
    db_my_duplicates.drop()   #the database collection where the found duplicates are stored gets dropped before every iteration
    for field_combination in check_field_combination:
        duplicates = modules.data_handler.find_duplicates(db_collection, field_combination) #finds duplicates for given field names
        modules.data_handler.safe_store_duplicates(db_my_duplicates, duplicates)            #and stores them in the database (unique)
    modules.data_handler.compare_duplicates(db_collection, db_given_duplicates, db_my_duplicates)   #compares found duplicates to given duplicates
    
    
print("\nProcess terminated after " + str(time.time() - start) + " seconds...")
    
    
    
    
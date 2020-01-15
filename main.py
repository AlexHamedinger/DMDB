#! python3
# main.py -----MAIN-METHODE-----    

import modules, time

start = time.time()

###################################* STEP 1 *####################################
#####LOAD RAW DATA FROM RESTAURANT_DATA INTO MONGO-DB AND GET DB-CONNECTIONS#####
modules.data_handler.my_print("STEP 1: Load data from files...")
#get data from restaurants.tsv and stores it in the database
data = modules.data_handler.get_data('restaurant_data/restaurants.tsv', '\t')  
db_collection = modules.data_handler.get_collection("dmdb_project", "restaurant_data") 
modules.data_handler.store_in_database(db_collection, data)   

#get data from restaurants_DUP.tsv and stores it in the database
given_duplicates = modules.data_handler.get_data('restaurant_data/restaurants_DPL.tsv', '\t')   
db_gold_duplicates = modules.data_handler.get_collection("dmdb_project", "restaurant_gold_duplicates")
modules.data_handler.store_in_database(db_gold_duplicates, given_duplicates)

#get database collection where the found duplicates will be stored
db_classifier_duplicates = modules.data_handler.get_collection("dmdb_project", "restaurant_classifier_duplicates")


###################* STEP 2 *####################
##### CLEAN THE FIELDS PHONE, NAME AND CITY #####
modules.data_handler.my_print("\n\nSTEP 2: Clean the fields...")
modules.data_handler.clean_phone(db_collection)
modules.data_handler.clean_city(db_collection)
modules.data_handler.clean_address(db_collection)
modules.data_handler.clean_type(db_collection)

##############* STEP 3 *##############
#####FIND DUPLICATES AND EVALUATE#####
#The following lists contain lists with combinations of field names which will be checked for duplicates
modules.data_handler.my_print("\n\nSTEP 3: Find duplicates and evaluate them...")


check_field_combination_0 = [["name", "address", "city"],   #all possible combinations of three
                             ["name", "address", "phone"],
                             ["name", "address", "type"],
                             ["name", "city", "phone"],
                             ["name", "city", "type"],
                             ["name", "phone", "type"],
                             ["address", "city", "phone"],
                             ["address", "city", "type"],
                             ["address", "phone", "type"],
                             ["city", "phone", "type"]]

check_field_combination_1 = [["name", "address", "city", "phone"],   #all possible combinations of four
                             ["name", "address", "city", "type"],
                             ["name", "address", "phone", "type"],
                             ["type", "address", "city", "phone"]]

check_field_combination_2 = [["name", "address", "city"],   #in my opinion the best balanced
                             ["address", "phone", "type"],  #could write about it in my report because of bad time management =( 
                             ["name", "phone"],
                             ["name", "city", "type"],
                             ["city", "phone", "type"]]


                                
all_field_combinations = []
all_field_combinations.append(check_field_combination_0)
all_field_combinations.append(check_field_combination_1)
all_field_combinations.append(check_field_combination_2)



#Loops through the field name combinations and evaluates them
for check_field_combination in all_field_combinations:
    print("\n")
    db_classifier_duplicates.drop()   #the database collection where the found duplicates are stored gets dropped before every iteration
    for field_combination in check_field_combination:
        duplicates = modules.data_handler.find_duplicates(db_collection, field_combination) #finds duplicates for given field names
        modules.data_handler.safe_store_duplicates(db_classifier_duplicates, duplicates)            #and stores them in the database (unique)
    modules.data_handler.compare_to_gold(db_collection, db_gold_duplicates, db_classifier_duplicates)   #compares found duplicates to given duplicates
    
    
print("\nProcess terminated after " + str(time.time() - start) + " seconds...")
    
    
    
    
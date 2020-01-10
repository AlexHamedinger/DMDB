# pipeline.py - For getting different pipelines by number


#returns types grouped by type            
def a():
    pipeline = [
                {'$group': {'_id': '$type', 
                            'count': {'$sum': 1}}}, 
                {'$sort': {'count': -1, 
                           '_id': 1}}
               ]
    return pipeline                    
            
#returns cities grouped by city            
def b():     
    pipeline = [
                {'$group': {'_id': '$city', 
                            'count': {'$sum': 1}}}, 
                {'$sort': {'count': -1, 
                           '_id': 1}}
               ]
    return pipeline
    
#returns documents with phone numbers of this type: 000/000-0000
def c():
    pipeline = [
                {'$match': {'phone': {'$regex': '[0-9]{3}/[0-9]{3}-[0-9]{4}'}}}
               ]
    return pipeline
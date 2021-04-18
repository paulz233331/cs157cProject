import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient()
db = client.testdb
movie_ratings = db.movie_ratings
mydict = {'genres': ['Action', 'Thriller'], 'title' :'Max Payne', 'year': 2008, 'ratings':[{'userId':59999, 'rating':5.0}], 'tags':[{'tagId':599,'tag':'Max Payne'}]}
obj = movie_ratings.insert_one(mydict)
pprint.pprint(obj.inserted_id)
myquery = {'title':'Jumanji'}
newvalues = { '$set':
              {'title':'Jumanji: Welcome to the Jungle', 'year':2017, 'genres':['Adventure','Comedy']}
            }
obj2 = movie_ratings.update_one(myquery,newvalues)
#obj = movie_ratings.find({'title':'Jumanji'}, {'tags':0, 'genome_tags':0, 'ratings':0})
pprint.pprint("Updated " + str(obj2.modified_count) + "document")
myquery2 = { "movieId": 99 }
obj3 = movie_ratings.delete_one(myquery2)
pprint.pprint("Deleted " + str(obj3.deleted_count) + " document")
    

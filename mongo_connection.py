import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient()

db = client.testdb
movie_ratings = db.movie_ratings
query = { "$text" : {"$search" : "Toy Story" } }
project = {"ratings":0, "tags":0, "genome_tags":0}

doc = movie_ratings.find(query,project).limit(5)
for x in doc:
    pprint.pprint(x)

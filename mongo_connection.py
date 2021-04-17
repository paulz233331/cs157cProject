import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient()

db = client.testdb
movie_ratings = db.movie_ratings
pprint.pprint(movie_ratings.find_one())

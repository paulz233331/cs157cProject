from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import warnings
from tmdbv3api import TMDb
from tmdbv3api import Movie

# Ignore deprecation warning for PyMongo's count() method
warnings.filterwarnings("ignore", category=DeprecationWarning)

def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()

def main():
    try:
        # Create connection
        client = MongoClient('localhost', 27021, serverSelectionTimeoutMS=3000, retryWrites=False)

        # Test connection
        client.admin.command('ismaster')

        # Start main loop
        main_loop(client.testdb.movie_ratings)
    except ConnectionFailure:
        print('Connection to MongoDB failed')


def main_loop(movie_ratings):
    print('Welcome to Movie Ratings Catalog')
    options = [
        '[1] Find a movie',
        '[2] Find the average rating for a movie',
        '[3] Find the movie count for each genre',
        '[4] Find movies for a genre',
        '[5] Find movies with tags',
        '[6] Find the highest rated movies for a genre',
        '[7] Find the highest rated movies for a year',
        '[8] Find the overview and popularity of a movie',
        '[9] Find the IMDb and TMDb IDs for a movie', #replace
        '[10] Find the cast members for a movie (probably change this)',
        '[11] Find the crew members for a movie (probably change this)',
        '[12] Find the lowest rated movies',
        '[13] Find the average movie ratings per year',
        '[14] Find the most relevant tags for a movie',
        '[15] Find the counts of each rating for a movie',
        '[16] Quit'
    ]
    quit_option = str(len(options))
    choice = ''

    # Keep looping until user quits
    while choice != quit_option:
        # Print divider for clearer separation
        print('--------------------------------------------------')

        # Print each option
        for option in options:
            print(option)

        # Get user input
        choice = input('Please choose a number: ')

        # Get corresponding function to call
        run_option = 'run_option_' + choice

        # Run the corresponding function if valid
        if run_option in globals():
            globals()[run_option](movie_ratings)
        elif choice != quit_option:
            print('Invalid choice, please try again')

    print('Thank you for using Movie Ratings Catalog')


def run_option_1(movie_ratings):
    # Prompt for title
    title = input('Please enter a movie title or movieId: ')
    if is_integer(title):
        results= movie_ratings.find(
            {'movieId': int(title)},
            {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        ).limit(5)
        for result in results:
            print(result)
    else:
        # Query MongoDB
        results = movie_ratings.find(
            {'title': title},
            {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        ).limit(5)
        # Print results
        for result in results:
            print(result)
        query = { "$text" : {"$search" : "\"" + title + "\""} }
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project).limit(5)
        print(str(results.count()) + ' similar movie(s) found for title: ' + title)
        for result in results:
            print(result)


def run_option_2(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    if is_integer(title):
        pipeline = [
            { "$match": { "movieId": int(title) } },
            { "$unwind": "$ratings" },
            { "$group": {
                "_id": {"movieId":"$movieId", "title":"$title" },
                "avgRatings": { "$avg": "$ratings.rating" }
                }
            }
        ]
        results=movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)
    else:
        pipeline = [
            { "$match": { "title": title } },
            { "$unwind": "$ratings" },
            { "$group": {
                "_id": {"movieId":"$movieId", "title":"$title" },
                "avgRatings": { "$avg": "$ratings.rating" }
                }
            }
        ]
        results=movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)

        pipeline = [
            { "$match": { "$text" : { "$search": "\"" + title + "\"" } } },
            { "$unwind": "$ratings" },
            { "$group": {
                "_id": {"movieId":"$movieId", "title":"$title" },
                "avgRatings": { "$avg": "$ratings.rating" }
                }
            },
            { "$limit" : 5 }
        ]
        results = movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)


def run_option_3(movie_ratings):
    pipeline = [
        {"$unwind": "$genres" },
        {"$group": {
            "_id": "$genres", 
            "count": { "$sum": 1 }
            }
        },
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "count": 1
            }
        }
    ]
    results = movie_ratings.aggregate(pipeline)
    for result in results:
        print(result)

def run_option_4(movie_ratings):
    genre = input('Please enter a genre or genres separated by space: ')
    genres = genre.split(' ')
    query = { "genres": { "$all": genres } }
    project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
    results = movie_ratings.find(query,project).limit(5)
    print(str(results.count()) + ' similar movie(s) found for genre(s): ' + genre)
    for result in results:
        print(result)

def run_option_5(movie_ratings):
    tag = input('Please enter a tag or tags separated by space: ')
    tags = tag.split(' ')
    query = { "$or": [
                { "tags.tag": { "$all": tags } },
                { "genome_tags.genome_tag": { "$all": tags } }
                ]
            }
    project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
    results = movie_ratings.find(query,project).limit(5)
    print(str(results.count()) + ' similar movie(s) found for tag(s): ' + tag)
    for result in results:
        print(result)


def run_option_6(movie_ratings):
    genre = input('Please enter a specific genre: ')
    pipeline = [
        { "$match": {"$expr": {"$in": [genre, "$genres"]}}},
        { "$unwind": "$ratings" },
        { "$group": {
            "_id": {"movieId":"$movieId", "title":"$title"},
            "avgRatings": { "$avg": "$ratings.rating" }
            }
        },{"$sort": {"avgRatings": -1}},
        { "$limit" : 5 }
    ]
    results = movie_ratings.aggregate(pipeline)
    for result in results:
        print(result)


def run_option_7(movie_ratings):
    year = input('Please enter a specific year: ')
    pipeline = [
        { "$match": {"year": int(year)}},
        { "$unwind": "$ratings" },
        { "$group": {
            "_id": {"movieId":"$movieId", "title":"$title"},
            "avgRatings": { "$avg":  "$ratings.rating" }
            }
        },
        {"$sort": {"avgRatings": -1}},
        { "$limit" : 5 }
    ]

    results = movie_ratings.aggregate(pipeline)
    for result in results:
        print(result)


def run_option_8(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    tmdbId = 0
    if is_integer(title):
        query = {"movieId" : int(title)}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    else:
        query = {"title" : title}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    
    #connect to tmdb
        
    tmdb = TMDb()
    tmdb.api_key = '9bcd808ab6fc59d3ed747c98701b9e3f'
    tmdb.language = 'en'
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId) 
    print(m.title + ". " + m.overview)
    print("Popularity: " + str(m.popularity))
    if is_integer(title):
    	results = movie_ratings.update_one({'movieId' : int(title)},
    	    { '$set' : { "overview" : m.overview, "popularity" : m.popularity } })
    else:
    	results = movie_ratings.update_one({'title' : title},
    	    { '$set' : { "overview" : m.overview, "popularity" : m.popularity  } })
    print( "Updated " + str(results.modified_count) + " document.")

def run_option_9(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    tmdbId = 0
    if is_integer(title):
        query = {"movieId" : int(title)}
        project = {'year':0, 'genres':0, 'ratings': 0, 'tags': 0, 'genome_tags':0}
        results = movie_ratings.find(query,project)
        for result in results:
            print(result)
    else:
        query = {"title" : title}
        project = {'year':0, 'genres':0, 'ratings': 0, 'tags': 0, 'genome_tags':0}
        results = movie_ratings.find(query,project)
        for result in results:
            print(result)

    
def run_option_10(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    tmdbId = 0
    if is_integer(title):
        query = {"movieId" : int(title)}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    else:
        query = {"title" : title}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    
    #connect to tmdb
        
    tmdb = TMDb()
    tmdb.api_key = '9bcd808ab6fc59d3ed747c98701b9e3f'
    tmdb.language = 'en'
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId)
    cast = getattr(m.casts,'cast','n/a')
    castmbrs = "Cast members of " + m.title + ": "
    for k in cast:
        castmbrs += k['name'] + ", "
    castmbrs = castmbrs[:-1]
    print(castmbrs)
    if is_integer(title):
    	results = movie_ratings.update_one({'movieId' : int(title)},
    	    { '$set' : { "cast" : castmbrs } })
    else:
    	results = movie_ratings.update_one({'title' : title},
    	    { '$set' : { "cast" : castmbrs } })
    print( "Updated " + str(results.modified_count) + " document.")


def run_option_11(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    tmdbId = 0
    if is_integer(title):
        query = {"movieId" : int(title)}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    else:
        query = {"title" : title}
        project = {'ratings': {"$slice":1}, 'tags': {"$slice":1}, 'genome_tags': {"$slice":1}}
        results = movie_ratings.find(query,project)
        for result in results:
            tmdbId = int(result['tmdbId'])
    
    #connect to tmdb
        
    tmdb = TMDb()
    tmdb.api_key = '9bcd808ab6fc59d3ed747c98701b9e3f'
    tmdb.language = 'en'
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId)
    crew = getattr(m.casts, 'crew','n/a')
    crwbrs = "Crew members of " + m.title + ": "
    for k in crew:
        crwbrs += k['name'] + ": " + k['department'] + " - " + k['job'] + ", "
    crwbrs = crwbrs[:-1]
    print(crwbrs)
    if is_integer(title):
    	results = movie_ratings.update_one({'movieId' : int(title)},
    	    { '$set' : { "crew" : crwbrs } })
    else:
    	results = movie_ratings.update_one({'title' : title},
    	    { '$set' : { "crew" : crwbrs } })
    print( "Updated " + str(results.modified_count) + " document.")


def run_option_12(movie_ratings):
    pass


def run_option_13(movie_ratings):
    pass


def run_option_14(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    if is_integer(title):
        pipeline = [
            { "$match": { "movieId": int(title) } },
            { "$unwind": "$genome_tags" },
            { "$group": {
                "_id": {"movieId":"$movieId", "title":"$title", "tag": "$genome_tags.genome_tag"},
                "maxRelevance": { "$max": "$genome_tags.relevance" }
                }
            },
            { "$sort": { "maxRelevance": -1 } },
            { "$limit" : 5 }
        ]
        results=movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)
    else:
        pipeline = [
            { "$match": { "title": title } },
            { "$unwind": "$genome_tags" },
            { "$group": {
                "_id": {"movieId":"$movieId", "title":"$title","tag": "$genome_tags.genome_tag"},
                "maxRelevance": { "$max": "$genome_tags.relevance" }
                }
            },
            { "$sort": { "maxRelevance": -1 } },
            { "$limit" : 5 }
        ]
        results=movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)

        pipeline = [
            { "$match": { "$text" : { "$search":  "\"" + title + "\"" } } },
            { "$unwind": "$genome_tags" },
            { "$group": {
                "_id": {"movieId":"$movieId","title":"$title", "tag": "$genome_tags.genome_tag"},
                "maxRelevance": { "$max": "$genome_tags.relevance" }
                }
            },
            { "$limit" : 5 }
        ]
        results = movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)


def run_option_15(movie_ratings):
    title = input('Please enter a movie title or movieId: ')
    if is_integer(title):
        pipeline = [
            { "$match": { "movieId" :int(title) } },
            { "$unwind": "$ratings" },
            { "$group": {
                "_id":  {"title":"$title",
			    "rating":"$ratings.rating"},
                "count": { "$sum": 1 }
                }
            },
            { "$group": {
                "_id": "$_id.title",
                "counts": {
                    "$push": {
                        "rating": "$_id.rating",
                        "count": "$count"
                    }
                }
            } }
        ]
        results = movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)
    else:
        pipeline = [
            { "$match": { "title" :title } },
            { "$unwind": "$ratings" },
            { "$group": {
                "_id":  {"title":"$title",
			    "rating":"$ratings.rating"},
                "count": { "$sum": 1 }
                }
            },
            { "$group": {
                "_id": "$_id.title",
                "counts": {
                    "$push": {
                        "rating": "$_id.rating",
                        "count": "$count"
                    }
                }
            } }
        ]
        results = movie_ratings.aggregate(pipeline)
        for result in results:
            print(result)


if __name__ == '__main__':
    main()

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import warnings
from tmdbv3api import TMDb, Movie

# Ignore deprecation warning for PyMongo"s count() method
warnings.filterwarnings("ignore", category=DeprecationWarning)

TMDB_API_KEY = "9bcd808ab6fc59d3ed747c98701b9e3f"


def main():
    try:
        # Create connection
        client = MongoClient("localhost", 27021, serverSelectionTimeoutMS=3000, retryWrites=False)

        # Test connection
        client.admin.command("ismaster")

        # Start main loop
        main_loop(client.testdb.movie_ratings)
    except ConnectionFailure:
        print("Connection to MongoDB failed")


def main_loop(movie_ratings):
    print("Welcome to Movie Ratings and Search")
    options = [
        "[1] Find a movie",
        "[2] Find the average rating for a movie",
        "[3] Find the movie count for each genre",
        "[4] Find movies for a genre",
        "[5] Find movies with tags",
        "[6] Find the highest rated movies for a genre",
        "[7] Find the highest rated movies for a year",
        "[8] Find the overview and popularity of a movie",
        "[9] Find the most common tags for a year",
        "[10] Find the cast members for a movie",
        "[11] Find the crew members for a movie",
        "[12] Find the movies in a genre with tags",
        "[13] Find the average movie ratings per year",
        "[14] Find the most relevant tags for a movie",
        "[15] Find the counts of each rating for a movie",
        "[16] Quit"
    ]
    quit_option = str(len(options))
    choice = ""

    # Keep looping until user quits
    while choice != quit_option:
        # Print divider for clearer separation
        print("--------------------------------------------------")

        # Print each option
        for option in options:
            print(option)

        # Get user input
        choice = input("Please choose a number: ")

        # Get corresponding function to call
        run_option = "run_option_" + choice

        # Run the corresponding function if valid
        if run_option in globals():
            globals()[run_option](movie_ratings)
        elif choice != quit_option:
            print(choice + "   " + quit_option)
            print("Invalid choice, please try again")

    print("Thank you for using Movie Ratings and Search")


def run_option_1(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    results = movie_ratings.find(
        get_movie_query(title_or_id),
        {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    ).limit(5)
    for result in results:
        print(result)

    # Similar movies
    if not is_integer(title_or_id):
        query = {"$text": {"$search": "\"" + title_or_id + "\""}}
        project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
        results = movie_ratings.find(query, project).limit(5)
        print(str(results.count()) + " similar movie(s) found for title: " + title_or_id)
        for result in results:
            print(result)


def run_option_2(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    results = movie_ratings.aggregate([
        {"$match": get_movie_query(title_or_id)},
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": {"movieId": "$movieId", "title": "$title"},
            "avgRatings": {"$avg": "$ratings.rating"}
        }}
    ])
    for result in results:
        print(result)

    # Similar movies
    if not is_integer(title_or_id):
        results = movie_ratings.aggregate([
            {"$match": {"$text": {"$search": "\"" + title_or_id + "\""}}},
            {"$unwind": "$ratings"},
            {"$group": {
                "_id": {"movieId": "$movieId", "title": "$title"},
                "avgRatings": {"$avg": "$ratings.rating"}
            }},
            {"$limit": 5}
        ])
        for result in results:
            print(result)


def run_option_3(movie_ratings):
    results = movie_ratings.aggregate([
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres",
            "count": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "count": 1
        }}
    ])
    for result in results:
        print(result)


def run_option_4(movie_ratings):
    genre = input("Please enter a genre or genres separated by space: ")
    genres = genre.split(" ")
    query = {"genres": {"$all": genres}}
    project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    results = movie_ratings.find(query, project).limit(5)
    print(str(results.count()) + " movie(s) found for genre(s): " + genre)
    for result in results:
        print(result)


def run_option_5(movie_ratings):
    tag = input("Please enter a tag or tags separated by space: ")
    tags = tag.split(" ")
    query = {
        "$or": [
            {"tags.tag": {"$all": tags}},
            {"genome_tags.genome_tag": {"$all": tags}}
        ]
    }
    project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    results = movie_ratings.find(query, project).limit(5)
    print(str(results.count()) + " movie(s) found for tag(s): " + tag)
    for result in results:
        print(result)


def run_option_6(movie_ratings):
    genre = input("Please enter a specific genre: ")
    results = movie_ratings.aggregate([
        {"$match": {"$expr": {"$in": [genre, "$genres"]}}},
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": {"movieId": "$movieId", "title": "$title"},
            "avgRatings": {"$avg": "$ratings.rating"}
        }},
        {"$sort": {"avgRatings": -1}},
        {"$limit": 5}
    ])
    for result in results:
        print(result)


def run_option_7(movie_ratings):
    year = input("Please enter a specific year: ")
    results = movie_ratings.aggregate([
        {"$match": {"year": int(year)}},
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": {"movieId": "$movieId", "title": "$title"},
            "avgRatings": {"$avg": "$ratings.rating"}
        }},
        {"$sort": {"avgRatings": -1}},
        {"$limit": 5}
    ])
    for result in results:
        print(result)


def run_option_8(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    tmdbId = 0
    query = get_movie_query(title_or_id)
    project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    results = movie_ratings.find(query, project)
    for result in results:
        tmdbId = int(result["tmdbId"])

    # Connect to TMDb
    tmdb = TMDb()
    tmdb.api_key = TMDB_API_KEY
    tmdb.language = "en"
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId)
    print(m.title + ". " + m.overview)
    print("Popularity: " + str(m.popularity))
    results = movie_ratings.update_one(get_movie_query(title_or_id),
                                       {"$set": {"overview": m.overview, "popularity": m.popularity}})
    print("Updated " + str(results.modified_count) + " document.")


def run_option_9(movie_ratings):
    year = input("Please enter a specific year: ")
    results = movie_ratings.aggregate([
        {"$match": {"year": int(year)}},
        {"$unwind": "$tags"},
        {"$group": {
            "_id": {"tag": "$tags.tag"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    for result in results:
        print(result)


def run_option_10(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    tmdbId = 0
    query = get_movie_query(title_or_id)
    project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    results = movie_ratings.find(query, project)
    for result in results:
        tmdbId = int(result["tmdbId"])

    # Connect to TMDb
    tmdb = TMDb()
    tmdb.api_key = TMDB_API_KEY
    tmdb.language = "en"
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId)
    cast = getattr(m.casts, "cast", "n/a")
    cast_members = "Cast members of " + m.title + ": "
    for k in cast:
        cast_members += k["name"] + ", "
    cast_members = cast_members[:-1]
    print(cast_members)
    results = movie_ratings.update_one(get_movie_query(title_or_id),
                                       {"$set": {"cast": cast_members}})
    print("Updated " + str(results.modified_count) + " document.")


def run_option_11(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    tmdbId = 0
    query = get_movie_query(title_or_id)
    project = {"ratings": {"$slice": 1}, "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}
    results = movie_ratings.find(query, project)
    for result in results:
        tmdbId = int(result["tmdbId"])

    # Connect to TMDb
    tmdb = TMDb()
    tmdb.api_key = TMDB_API_KEY
    tmdb.language = "en"
    tmdb.debug = True

    movie = Movie()
    m = movie.details(tmdbId)
    crew = getattr(m.casts, "crew", "n/a")
    crew_members = "Crew members of " + m.title + ": "
    for k in crew:
        crew_members += k["name"] + ": " + k["department"] + " - " + k["job"] + ", "
    crew_members = crew_members[:-1]
    print(crew_members)
    results = movie_ratings.update_one(get_movie_query(title_or_id),
                                       {"$set": {"crew": crew_members}})
    print("Updated " + str(results.modified_count) + " document.")


def run_option_12(movie_ratings):
    genre = input("Please enter a genre or genres separated by space: ")
    genres = genre.split(" ")
    tag = input("Please enter a tag or tags separated by space: ")
    tags = tag.split(" ")
    results = movie_ratings.find({"genres": {"$all": genres},
                                  "$or": [{"tags.tag": {"$all": tags}},
                                          {"genome_tags.genome_tag": {"$all": tags}}
                                          ]}, {"ratings": {"$slice": 1},
                                               "tags": {"$slice": 1}, "genome_tags": {"$slice": 1}}).limit(5)
    print(str(results.count()) + " movie(s) found for tag(s) " + str(tags) + " in genre(s) " + str(genres))
    for result in results:
        print(result)


def run_option_13(movie_ratings):
    results = movie_ratings.aggregate([
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": "$year",
            "avgRatings": {"$avg": "$ratings.rating"}
        }},
        {"$project": {
            "_id": 0,
            "year": "$_id",
            "avgRatings": 1
        }},
        {"$sort": {"year": -1}}
    ])
    for result in results:
        print(result)


def run_option_14(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    results = movie_ratings.aggregate([
        {"$match": get_movie_query(title_or_id)},
        {"$unwind": "$genome_tags"},
        {"$group": {
            "_id": {"movieId": "$movieId", "title": "$title", "tag": "$genome_tags.genome_tag"},
            "maxRelevance": {"$max": "$genome_tags.relevance"}
        }},
        {"$sort": {"maxRelevance": -1}},
        {"$limit": 5}
    ])
    for result in results:
        print(result)

    # Similar movies
    if not is_integer(title_or_id):
        results = movie_ratings.aggregate([
            {"$match": {"$text": {"$search": "\"" + title_or_id + "\""}}},
            {"$unwind": "$genome_tags"},
            {"$group": {
                "_id": {"movieId": "$movieId", "title": "$title", "tag": "$genome_tags.genome_tag"},
                "maxRelevance": {"$max": "$genome_tags.relevance"}
            }},
            {"$limit": 5}
        ])
        for result in results:
            print(result)


def run_option_15(movie_ratings):
    title_or_id = input("Please enter a movie title or movieId: ")
    results = movie_ratings.aggregate([
        {"$match": get_movie_query(title_or_id)},
        {"$unwind": "$ratings"},
        {"$group": {
            "_id": {"title": "$title",
                    "rating": "$ratings.rating"},
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": "$_id.title",
            "counts": {
                "$push": {
                    "rating": "$_id.rating",
                    "count": "$count"
                }
            }
        }}
    ])
    for result in results:
        print(result)


def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def get_movie_query(title_or_id):
    return {"movieId": int(title_or_id)} if is_integer(title_or_id) else {"title": title_or_id}


if __name__ == "__main__":
    main()

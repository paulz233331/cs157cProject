from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import warnings

# Ignore deprecation warning for PyMongo's count() method
warnings.filterwarnings("ignore", category=DeprecationWarning)


def main():
    try:
        # Create connection
        client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=3000)

        # Test connection
        client.admin.command('ismaster')

        # Start main loop
        main_loop(client.moviedb.movie_ratings)
    except ConnectionFailure:
        print('Connection to MongoDB failed')


def main_loop(movie_ratings):
    print('Welcome to Movie Ratings Catalog')
    options = [
        '[1] Find a movie',
        '[2] Find the average rating for a movie',
        '[3] Find the movie count for each genre',
        '[4] Find movies with tags',
        '[5] Find movies for a genre',
        '[6] Find the highest rated movies for a genre',
        '[7] Find the highest rated movies for a year',
        '[8] Find the overview for a movie',
        '[9] Find the IMDb and TMDb IDs for a movie',
        '[10] Find the cast members for a movie (probably change this)',
        '[11] Find the crew members for a movie (probably change this)',
        '[12] Find the lowest rated movies',
        '[13] Find the average movie ratings per year',
        '[14] Find the movie and genome tags with the highest relevance scores',
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
    title = input('Please enter a movie title: ')

    # Query MongoDB
    results = movie_ratings.find(
        {'title': title},
        {'ratings': 0, 'tags': 0, 'genome_tags': 0}
    ).limit(5)

    # Print results
    print(str(results.count()) + ' movie(s) found for title: ' + title)
    for result in results:
        print(result)


def run_option_2(movie_ratings):
    pass


def run_option_3(movie_ratings):
    pass


def run_option_4(movie_ratings):
    pass


def run_option_5(movie_ratings):
    pass


def run_option_6(movie_ratings):
    pass


def run_option_7(movie_ratings):
    pass


def run_option_8(movie_ratings):
    pass


def run_option_9(movie_ratings):
    pass


def run_option_10(movie_ratings):
    pass


def run_option_11(movie_ratings):
    pass


def run_option_12(movie_ratings):
    pass


def run_option_13(movie_ratings):
    pass


def run_option_14(movie_ratings):
    pass


def run_option_15(movie_ratings):
    pass


if __name__ == '__main__':
    main()

from tmdbv3api import TMDb
from tmdbv3api import Movie

tmdb = TMDb()
tmdb.api_key = '9bcd808ab6fc59d3ed747c98701b9e3f'
tmdb.language = 'en'
tmdb.debug = True

movie = Movie()
m = movie.details(862) #toy story 
print(m.title)
print(m.overview)
print(m.popularity)
#print(m.casts)
cast = getattr(m.casts,'cast','n/a')
crew = getattr(m.casts, 'crew','n/a')
castmbrs = "Cast members: "
for k in cast:
    castmbrs += k['name'] + ", "
castmbrs = castmbrs[:-1]
crwbrs = "Crew members: "
for k in crew:
    crwbrs += k['name'] + ": " + k['department'] + " - " + k['job'] + ", "
crwbrs = crwbrs[:-1]

print(castmbrs)
print(crwbrs)

##Toy Story
##Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.
##98.308
##Cast members: Tom Hanks, Tim Allen, Don Rickles, Jim Varney, Wallace Shawn, John Ratzenberger, Annie Potts, John Morris, Erik von Detten, Laurie Metcalf, R. Lee Ermey, Sarah Freeman, Penn Jillette, Jack Angel, Spencer Aste, Greg Berg, Lisa Bradley, Kendall Cunningham, Debi Derryberry, Cody Dorkin, Bill Farmer, Craig Good, Gregory Grudt, Danielle Judovits, Sam Lasseter, Brittany Levenbrown, Sherry Lynn, Scott McAfee, Mickie McGowan, Ryan O'Donohue, Jeff Pidgeon, Patrick Pinney, Phil Proctor, Jan Rabson, Joe Ranft, Andrew Stanton, Shane Sweet, Nathan Lane, John Lasseter, Ernie Sabella,
##Crew members: Andrew Stanton, Andrew Stanton, Andrew Stanton, Lee Unkrich, Graham Walters, Graham Walters, John Lasseter, John Lasseter, Mike Fenton, Gary Rydstrom, Gary Rydstrom, Janet Hirshenson, Don Davis, Pat Jackson, Jeff Pidgeon, Ralph Eggleston, Randy Newman, Randy Newman, Thomas Porter, Tia W. Kratter, Glenn McQueen, Rich Quade, Eben Ostby, Ruth Lambert, Joe Ranft, Bud Luckey, Bud Luckey, Bud Luckey, Jonas Rivera, Robin Cooper, Norm DeCarlo, Doug Sweetland, Shawn Krause, Jeff Pratt, Michael Berenstein, Jimmy Hayward, Karen Kiser, David Tart, Alan Sperling, Dan Engstrom, Deirdre Warin, Deirdre Warin, Mark Adams, Mark Adams, Daniel McCoy, Anthony A. Apodaca, Anthony A. Apodaca, Anthony A. Apodaca, Rob Cook, Louis Rivera, Don Conway, Doc Kane, Dennie Thorpe, Tony Eckert, Tim Holland, Frank Welker, Pete Docter, Pete Docter, Joss Whedon, Joel Cohen, Alec Sokolow, Bonnie Arnold, Ed Catmull, Ralph Guggenheim, Steve Jobs, Ash Brannon, Karen Robert Jackson, Sharon Calahan, William Cone, William Cone, Tom Myers, Galyn Susman, Kevin Reher, Mickie McGowan, Colin Brady, Mark Oftedal, Guionne Leroy, Chris Montan, Robert Lence, Tom Holloway, Meredith Layne, Robert Gordon, Dan Haskett, Steve Rabatich, Dana Mulligan, James Flamberg, Mark Dornfeld, Lucas Putnam, Mark Thomas Henne, J.R. Grubbs, Marilyn McCoppen, Gary Summers, Hal T. Hickel, Dale E. Grahn, Sonoko Konishi, Tom Barwick, Jean Gillmore, Kim Blanchette, Angie Glocka, Ken Willard, Victoria Livingstone, Susan Bradley, Lauren Beth Strogoff, Steve Segal, Mary Helen Leasman, Robin Lee, Patsy Bouge, Triva von Klark, Rich Mackay, Mary Beth Smith, Rick Mackay, William Reeves, Davey Crockett Feiten, Rex Grignon, Tom K. Gurney, Anthony B. LaMolinara, Les Major, Doug Sheppeck, Oren Jacob, Darwyn Peachey, Mitch Prater, Brian M. Rosen, Shelley Daniels Lekven, Steve Johnson, Roman Figun, Desirée Mourad, Kelly O'Connell, Ann M. Rockwell, Julie M. McDonald, Tom Freeman, Ada Cochavi, Deirdre Morrison, Lori Lombardo, Ellen Devine, Susan Sanford, Susan Popovic, Christian Hill, Terri Greening, Miguel Ángel Poveda, B.Z. Petroff, Matthew Luhn, Cynthia Dueltgen, Matthew Martin, Ewan Johnson, Ewan Johnson, Victoria Jaschob, Don Schreiter, Mark T. VandeWettering, Mark T. VandeWettering, Maureen Wylie, Terry McQueen, Douglas Todd, David H. Ching, Bob Baron, Jesse William Wallace, Lisa Ellis, Kevin Bjorke, Barbara T. Labounta, Michael E. Murdock, Deborah R. Fowler, Damir Frkovic, Damir Frkovic, Shalini Govil-Pai, Shalini Govil-Pai, David R. Haumann, David R. Haumann, Yael Milo, Yael Milo, Tod Cooper, Andrew Caldwell, Martin Caplan, Ryan Chisum, Takeshi Hasegawa, Jay Hathaway, Jason Henry, Steven Kani, Kevin Page, Benjamin Salles, Dave Thomas, Nancy Copeland, Alethea Harampolis, Pat Hanrahan, Jim Lawson, M.W. Mantle, David Salesin, Keith B.C. Gordon, Larry Gritz, Larry Gritz, Bill Carson, Ken Huey, Mark Eastwood, Monique Hodgkinson, Grey Holland, Larry Aupperle, Heather Knight, Roger Rose,

##PS C:\Users\paulz\Documents\ml-latest> gc movies.csv |select -first 10
##movieId,title,genres
##1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
##2,Jumanji (1995),Adventure|Children|Fantasy
##3,Grumpier Old Men (1995),Comedy|Romance
##4,Waiting to Exhale (1995),Comedy|Drama|Romance
##5,Father of the Bride Part II (1995),Comedy
##6,Heat (1995),Action|Crime|Thriller
##7,Sabrina (1995),Comedy|Romance
##8,Tom and Huck (1995),Adventure|Children
##9,Sudden Death (1995),Action
##PS C:\Users\paulz\Documents\ml-latest> gc links.csv |select -first 10
##movieId,imdbId,tmdbId
##1,0114709,862
##2,0113497,8844
##3,0113228,15602
##4,0114885,31357
##5,0113041,11862
##6,0113277,949
##7,0114319,11860
##8,0112302,45325
##9,0114576,9091

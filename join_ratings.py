import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', None)

data1 = pd.read_csv('movies.csv')
data2 = pd.read_csv('links.csv')
data3 = pd.read_csv('ratings.csv')
data4 = pd.read_csv('tags.csv')
data5 = pd.read_csv('genome_scores.csv')
data6 = pd.read_csv('genome_tags.csv')

#combine ratings into array by movieId.
data1 = data1.groupby(['movieId']).agg(list)
data1b = pd.DataFrame(columns = ['movieId','title','year','genres'])
for movieId, row in data1.iterrows():
    title = row[0][0][0:-7]
    year = row[0][0][-5:-1]
    data1b = data1b.append({'movieId': movieId, 'title':title, 'year' : year, 'genres' : row[1][0]},ignore_index=True)
#print(data1b.head())
    
#combine ratings into array by movieId.
data3 = data3.groupby(['movieId']).agg(list)
data3b = pd.DataFrame(columns = ['movieId','ratings'])

for movieId, row in data3.iterrows():
    arr = []  
    for i in range(len(row[0])):
        arr.append([row[0][i],row[1][i]])
    data3b = data3b.append({'movieId':movieId, 'ratings':arr}, ignore_index=True) 

#combine tags into array by movieId
data4 = data4.groupby(['movieId']).agg(list)
data4b = pd.DataFrame(columns = ['movieId','tags'])

for movieId, row in data4.iterrows():
    arr = []
    for i in range(len(row[0])):
        arr.append([row[0][i],row[1][i]])
    data4b = data4b.append({'movieId':movieId, 'tags': arr}, ignore_index =True)

#combine genome tags into array by movieId
data5 = data5.merge(data6, how='left', on='tagId', right_index=False)
#print(data5.head())

data5 = data5.groupby(['movieId']).agg(list)
data5b = pd.DataFrame(columns = ['movieId','genome_tags'])
for movieId, row in data5.iterrows():
    arr = []
    for i in range(len(row[0])):
        arr.append([row[2][i],row[1][i]])
    data5b = data5b.append({'movieId': movieId, 'genome_tags':arr},ignore_index=True)
#print(data5b.head())

df1 = data1b.merge(data2, how='left', on='movieId')
#combine tags into movies
df1 = df1.merge(data4b, how='left', on='movieId', right_index=False)
#combine ratings into movies
df1 = df1.merge(data3b, how='left', on='movieId', right_index=False)
#combine genome tags into movies
df1 = df1.merge(data5b, how='left', on='movieId', right_index=False)

#split genres into array
#df3["genres"] = df3["genres"].str.split('|')

print(df1.head())
df1.to_csv('movie_ratings.csv',index=False)


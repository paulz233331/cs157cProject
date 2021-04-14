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

data1 = data1.groupby(['movieId']).agg(list)
data1b = pd.DataFrame(columns = ['movieId','title','year','genres'])
for movieId, row in data1.iterrows():
    title = row[0][0][0:-6]
    year = row[0][0][-5:-1]
    data1b = data1b.append({'movieId': movieId, 'title':title, 'year' : year, 'genres' : row[1][0]},ignore_index=True)
print(data1b.head())
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

df1 = data1b.merge(data2, how='left', on='movieId')
#combine tags into movies
df2 = df1.merge(data4b, how='left', on='movieId', right_index=False)
#combine ratings into movies
df3 = df2.merge(data3b, how='left', on='movieId', right_index=False)
#split genres into array
df3["genres"] = df3["genres"].str.split('|')

print(df3.head())
df3.to_csv('movie_ratings.csv',index=False)


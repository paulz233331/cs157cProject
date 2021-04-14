import pandas as pd
import json
import re
import math

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', None)

#make json format of tags
data1 = pd.read_csv('movie_ratings.csv')
data1 = data1.groupby(['movieId']).agg(list)
data1b = pd.DataFrame(columns = ['movieId','tags'])

#for item in data1['tags']:
for movieId, row in data1.iterrows():
    item = row['tags'][0]
    item2 = row['ratings'][0]
#    print(item2)
    if isinstance(item,float) and math.isnan(item):
        continue;
    else:
        items = []
        item = item.replace('[', '')
        item = item.replace(']', '')
#        print(item)
        v = item.split(',')
        i =0
        while i < int((len(v)+.5)/2):
#            print(v[i])
#            print (v[i+1])
            items.append({'userId': v[i], 'tag':v[i+1] })
            i +=2
#        print(items)
        data1b = data1b.append({'movieId':movieId, 'tags':items}, ignore_index=True)
        #input()
        #print(data1b)

#make json format of ratings
data1c = pd.DataFrame(columns = ['movieId','ratings'])

for movieId, row in data1.iterrows():
    item = row['ratings'][0]
    if isinstance(item,float) and math.isnan(item):
        continue;
    else:
        items = []
        item = item.replace('[', '')
        item = item.replace(']', '')
        v = item.split(',')
        i =0
        while i < int((len(v)+.5)/2):
            items.append({'userId': v[i], 'rating':v[i+1] })
            i +=2
        data1c = data1c.append({'movieId':movieId, 'ratings':items}, ignore_index=True)



data1 = pd.read_csv('movie_ratings.csv')
data1 = data1.drop(columns=['tags','ratings'])
data1["genres"] = data1["genres"].str.split('|')
df1 = data1.merge(data1b, how='left', on='movieId', right_index=False)
df1 = df1.merge(data1c, how='left', on='movieId', right_index=False)

print(df1.head())

out = df1.to_json(orient='records')

with open('movie_ratings.json', 'w') as f:
    f.write(out)


#30 minutes
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

for movieId, row in data1.iterrows():
    item = row['tags'][0]
    if isinstance(item,float) and math.isnan(item):
        continue;
    else:
        items = []
        item = item.replace('[', '')
        item = item.replace(']', '')
        v = item.split(',')
        i =0
        while i < int((len(v)+.5)/2):
            v[i] = v[i].replace(' ','')
            v[i+1] = v[i+1].replace('\'','')
            v[i+1] = v[i+1].replace(' ','')
            if v[i].strip().isdigit() and v[i+2].strip().isdigit():
                items.append({'userId': int(v[i]), 'tag':v[i+1], 'timestamp':int(v[i+2]) })        
            else:
                j = i
                while not v[i+2].strip().isdigit():
                    v[j+1] = v[j+1] + v[j+2].replace('\'','')
                    v[j+2] = v[i+3]
                    i+=1
                items.append({'userId': int(v[j]), 'tag':v[j+1], 'timestamp':int(v[j+2]) })
            i +=3
        data1b = data1b.append({'movieId':movieId, 'tags':items}, ignore_index=True)

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
            items.append({'userId': int(v[i]), 'rating': float(v[i+1]), 'timestamp':int(v[i+2]) }) #revision1
            i +=3
        data1c = data1c.append({'movieId':movieId, 'ratings':items}, ignore_index=True)

#make json format of genome tags
data1d = pd.DataFrame(columns = ['movieId','genome_tags'])

for movieId, row in data1.iterrows():
    item = row['genome_tags'][0]
    if isinstance(item,float) and math.isnan(item):
        continue;
    else:
        items = []
        item = item.replace('[', '')
        item = item.replace(']', '')
        v = item.split(',')
        v[i+1] = v[i+1].replace('\'','')
        v[i+1] = v[i+1].strip()
        i =0
        while i < int((len(v)+.5)/2):
            items.append({'tagId': int(v[i]), 'genome_tag': v[i+1], 'relevance':float(v[i+2]) })
            i +=3
        data1d = data1d.append({'movieId':movieId, 'genome_tags':items}, ignore_index=True)


#drop columns
data1 = pd.read_csv('movie_ratings.csv')
data1 = data1.drop(columns=['tags','ratings','genome_tags'])
#split genres into array
data1["genres"] = data1["genres"].str.split('|')
#merge JSON formatted columns for nested documents
df1 = data1.merge(data1b, how='left', on='movieId', right_index=False)
df1 = df1.merge(data1c, how='left', on='movieId', right_index=False)
df1 = df1.merge(data1d, how='left', on='movieId', right_index=False)
print(df1.head())

out = df1.to_json(orient = 'records')

with open('movie_ratings.json', 'w') as f:
    f.write(out)


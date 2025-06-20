import os
import json
import http.client
import pandas as pd
from google.cloud import bigquery


if "BEARER" not in os.environ:
    print('Bearer token not found')
    exit()
token = os.environ["BEARER"] 

db = bigquery.Client()

with open('request.json') as f:
    payload = json.load(f)

payload[0]['variables']['filters']['limit'] = 1000
payload[0]['variables']['filters']['offset'] = 0

api = http.client.HTTPSConnection('www.englishdom.com')
api.request('POST', '/api/', json.dumps(payload), {
    'Content-type': 'application/json',
    'x-accept-language': 'ua',
    "authorization1":f"Bearer {token}"
    })

response = api.getresponse()
if response.status != 200:
    print(f'{response.status} {response.reason}')
    exit() 
    
response = response.read().decode()
data = json.loads(response)

df = pd.json_normalize(data[0]['data']['getTeacherSelection']['data'])

def filter(key, val):
    def closure(a):
        return any(x[key] == val for x in a)
    return closure

filtered = df[df['attributes.preparation_programs'].apply(filter('key','it'))]
print(f'{filtered["attributes.user_id"].count()} out of {df["attributes.user_id"].count()} teachers were found')

bulk = []
bulk.append("TRUNCATE TABLE `metlinskyi.englishdom.teachers`")
for row in filtered.to_records():
    bulk.append(f"INSERT INTO `metlinskyi.englishdom.teachers` VALUES(\
    '{row['attributes.teacher_alias']}',\
    '{row['attributes.country']}',\
    '{str(row['attributes.description']).replace("'","\\'").replace("\n","")}',\
    '{row['attributes.avatar']}'\
    )")

query_job = db.query(";\n".join(bulk))
result = query_job.result()
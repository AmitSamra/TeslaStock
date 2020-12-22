import requests 
import base64
import json
import os
import csv
from datetime import datetime, date


############################################################
# Authentication

# Load Keys
client_key = os.environ.get('CLIENT_KEY')
client_secret = os.environ.get('CLIENT_SECRET')

# Encode keys using base64
key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
b64_encoded_key = base64.b64encode(key_secret)
b64_encoded_key = b64_encoded_key.decode('ascii')

# POST request to auth endpoint to obtain Bearer Token
base_url = 'https://api.twitter.com/'
auth_url = '{}oauth2/token'.format(base_url)

auth_headers = {
    'Authorization': 'Basic {}'.format(b64_encoded_key),
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
}

auth_data = {
    'grant_type': 'client_credentials'
}

auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)
access_token = auth_resp.json()['access_token']


############################################################
# Search

# Make queries
search_headers = {
    'Authorization': 'Bearer {}'.format(access_token)    
}

search_params = {
    'query': 'Tesla',
    'fromDate': '202012010000',
    'toDate': '202012060000',
    'bucket': 'day'
}

search_url = '{}1.1/tweets/search/fullarchive/full/counts.json'.format(base_url)
search_resp = requests.get(search_url, headers=search_headers, params=search_params)
tweet_data = search_resp.json()


# Save results in JSON file
with open('./tsla_tweet_count.json', 'w') as f:
    json.dump(tweet_data, f, indent=4, sort_keys=True)


# Save only releveant portion of request
relevant_data = []
with open('./tsla_tweet_count.json', 'r') as f:
    data = json.load(f)

for i in data['results']:
    relevant_data.append({'tweet_date':i['timePeriod'][0:8], 'tweet_count':i['count']})

# Convert tweet_date to datetime object
for i in relevant_data:
    i['tweet_date'] = datetime.strptime(i['tweet_date'], '%Y%m%d')

# Reformat date
for i in relevant_data:
    i['tweet_date']=datetime.strftime(i['tweet_date'],'%Y-%m-%d')



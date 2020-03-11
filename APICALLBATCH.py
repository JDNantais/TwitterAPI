#using responses, cURL and snakebite to access Hadoop Windows Instance VM and put files into HDFS folder.
import os
import requests
import json
from snakebite.client import Client
#hadoop connection
client1 = Client('localhost',19000)
#Batch Twitter API connection
endpoint = "https://api.twitter.com/1.1/tweets/search/fullarchive/HistoricalTweets.json" 
headers = {"Authorization":"Bearer xxxxxx", "Content-Type": "application/json"} 
# change your query here: 
data = '{"query":"(AI OR Artificial Intelligence OR Machine Learning)", "fromDate": "201602020000", "toDate": "201902240000" , "maxResults":10}'
response = requests.post(endpoint,data=data,headers=headers).json()
file = json.dumps(response, indent = 2)
# put downloaded data into local disk temporarily
with open('data.txt', 'w') as outfile:
    json.dump(file, outfile)
#for p in client1.mkdir(['/twitter_data_hist']):
#    print(p)
#for x in client1.ls(['/']):
#    print(x)
# this sends the file to hadoop
cmd = "hdfs dfs -put C:\\Users\\user\\Desktop\\data_603_Twitter_API\\data.txt /twitter_data_hist/twitterhistpy.json"
os.system(cmd)
# this removes the file from local directory
os.remove('data.txt')

import os
from TwitterAPI import TwitterAPI, TwitterResponse, TwitterPager
from pymongo import MongoClient, DESCENDING
from pymongo.errors import DuplicateKeyError
from datetime import datetime
import math

class TwitterTimeline(object): 
    """
    docstring
    """
    def __init__(self, consumer_key, consumer_secret, access_token_key, access_token_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token_key = access_token_key
        self.access_token_secret = access_token_secret        

    def get_api(self) -> TwitterAPI:
        if self.consumer_key != None and self.consumer_secret != None and self.access_token_key != None and self.access_token_secret != None:
            return TwitterAPI(self.consumer_key, self.consumer_secret, self.access_token_key, self.access_token_secret)
        else:
            raise ValueError("Required enviroment variables not set!")

    def get_timeline(self, count: int = 20, since: int = None, max: int = None) -> TwitterPager:
        api = self.get_api()
        if api != None:        
            pager = TwitterPager(api, 'statuses/home_timeline', self.query_params(count, since, max))
            return pager
        else:
            raise ValueError("API is not set!")        

    def query_params(self, count: int, since: int, max: int): 
        params = {}
        params['count'] = count
        params['tweet_mode'] = 'extended'
        if since != None:
            params['since_id'] = since
        if max != None:
            params['max_id'] = max
        return params

if __name__ == "__main__":
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')
    access_token_key = os.getenv('ACCESS_TOKEN_KEY')
    access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')    
    timeline = TwitterTimeline(consumer_key, consumer_secret, access_token_key, access_token_secret)

    mongo_url = os.getenv('MONGO_URL')
    if mongo_url == None or mongo_url == '':
        mongo_url = 'mongodb://localhost:27017/'

    mongo_client = MongoClient(mongo_url)

    timeline_db = mongo_client.db_timeline
    tweets_collection = timeline_db.tweets

    control_collection = timeline_db.control
    last_run = control_collection.find_one({},sort=[('runDate', DESCENDING)])
    
    if last_run != None:
        since_id = last_run['maxId']
    else:
        since_id = None
    
    max_id = None
    
    tweet_pages = timeline.get_timeline(200, since_id)    
    for tweet in tweet_pages.get_iterator():
        if 'full_text' in  tweet:
            if max_id == None:
                max_id = tweet['id']
            else:
                if tweet['id'] > max_id:
                    max_id = tweet['id']
            tweet['_id'] = tweet['id']
            try:
                tweets_collection.replace_one(tweet, tweet, upsert=True)
            except DuplicateKeyError:
                print("Duplicate tweet ", tweet, " ,moving on")
                continue
        elif 'message' in tweet:
            print("got error", tweet)
            break

    if max_id != None:
        control_collection.insert_one({'runDate': datetime.now(), 'maxId': max_id})
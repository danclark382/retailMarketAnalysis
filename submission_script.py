import csv
import datetime as dt
import json
import os
import time

from pprint import pprint
import praw
from praw.models import MoreComments
from psaw import PushshiftAPI
from pymongo import MongoClient

with open("keys.json", "r") as f:
    keys = json.load(f)
with open("response_filters.json", "r") as f:
    filters = json.load(f)


pop_path = "data/popularity_export/"
filters = [key for key in filters if filters[key] == 1]
reddit_keys = keys["keys"]["reddit"]['script']
mon_user = keys["keys"]["mongo"]['username']
mon_secret = keys["keys"]["mongo"]['secret']
client = MongoClient(f"mongodb+srv://{mon_user}:{mon_secret}@cluster0.vvy6o.mongodb.net/<dbname>?retryWrites=true&w=majority")


def get_submissions(start_date, ct=0):
    if ct >= 2000:
        time.sleep(30)
        ct = 0
    start_epoch = int(start_date.timestamp())
    if str(start_date)[0:10] == str(dt.datetime.today())[0:10]:
        return
    created_at = None
    sub = None
    for submission in api.search_submissions(after=start_epoch,
                                             before=int((start_date+add_day).timestamp()),
                                             subreddit='wallstreetbets',
                                             limit=500):
        post_id = wsb_coll.insert_one(submission.d_).inserted_id
        created_at = submission.d_['created']
        
        ct += 1
    time.sleep(.2)
    get_submissions(dt.datetime.fromtimestamp(created_at), ct)

    
def get_min_date(path):
    pop_file_lst = [os.path.join(pop_path, i) for i in os.listdir(pop_path) if i.endswith('.csv')]
    first_rep_date = []
    for i in pop_file_lst:
        with open(i, 'r') as csvfile:
            pop_csv = csv.reader(csvfile, delimiter=',')
            for ix, line in enumerate(pop_csv):
                if ix == 1:
                    first_rep_date.append(line[0][:10].replace('-', ''))
                    break
    min_date = min(first_rep_date)
    min_date = dt.datetime(int(min_date[0:4]), int(min_date[4:6]), int(min_date[6:8]))
    return min_date


api = PushshiftAPI()
first_rec_date = get_min_date(pop_path)
add_day = dt.timedelta(days=1)
first_query_date = first_rec_date - dt.timedelta(days=7)

db = client['test']
wsb_coll = db['test_coll']
start_epoch = int(first_query_date.timestamp())
get_submissions(dt.datetime(2020,8,15)) 
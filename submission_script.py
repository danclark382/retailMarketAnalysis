from bson.objectid import ObjectId
from contextlib import contextmanager
import csv
from datetime import datetime, timedelta
import json
import glob
import os
from pprint import pprint
import signal
import sys
import time

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
start_time = datetime.now()
reddit = praw.Reddit(client_id=reddit_keys['client_id'],
                     client_secret=reddit_keys['secret_key'],
                     user_agent=reddit_keys['name'])


def get_minutes(start):
    td = datetime.now() - start
    if (td.seconds//60)%60 >= 20:
        return True
    return False


@contextmanager
def timeout(time):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(time)
    try:
        yield
    except TimeoutError:
        sys.exit(1)
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def raise_timeout(signum, frame):
    raise TimeoutError
    

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
    min_date = datetime(int(min_date[0:4]), int(min_date[4:6]), int(min_date[6:8]))
    return min_date


def get_last_record():
    with open("last_id.json", "r") as f:
        data = json.load(f)
    return data['_id']


def dump_records(file, data):
    with open(f'data/reddit_submissions/{file}.json', 'w') as fp:
        json.dump(data, fp)


def get_recent_date(coll):
    return datetime.fromtimestamp(coll.find_one({"_id": ObjectId(get_last_record())})['created'])


def get_last_file():
    file_list = glob.glob('data/reddit_submissions/*.json')
    most_recent = max(file_list, key=os.path.getctime)
    date = [int(x[1:]) if x[0] == '0' else int(x) for x in most_recent.split('/')[-1].split('.')[0].split('_')]
    return datetime(date[2], date[0], date[1])
    
def get_submissions(start_date):
    with timeout(300):
        print(start_date)
        start_epoch = int(start_date.timestamp())
        if str(start_date)[0:10] == str(datetime.today())[0:10]:
            sys.exit(1)
        created_at = None
        sub = None
        results = {'data': []}
        try:
            
            for submission in api.search_submissions(after=start_epoch,
                                                     before=int((start_date + timedelta(days=1)).timestamp()),
                                                     subreddit='wallstreetbets'):
                doc = submission.d_
                score = get_score(doc['id'])
                if score:
                    doc['score'] = score
                results['data'].append(doc)
                #post_id = wsb_coll.insert_one(submission.d_).inserted_id
                created_at = submission.d_['created']
                #with open("last_id.json", "w") as outf: 
                #    outf.write(json.dumps({"_id": str(post_id)}, indent=4))
        except Exception:
            sys.exit(0)
            
        dump_records(start_date.strftime("%m_%d_%Y"), results)
        return datetime.fromtimestamp(created_at)


def get_score(post_id):
    try:
        submission = reddit.submission(id=post_id)
    except Exception:
        with open('error_ids.txt', 'w+') as f:
            f.writeline(post_id + "\n")
            return False
    return submission.score
    

    

api = PushshiftAPI()
db = client['social_media']
wsb_coll = db['reddit_submissions']
#last_record = get_recent_date(wsb_coll)
last_record = get_last_file()
#last_record = datetime(2020, 8, 30, 23, 0)
while last_record != datetime.now().timestamp():
    last_record = get_submissions(last_record)

from contextlib import contextmanager
import glob
import json
import os
import signal

import praw


with open("keys.json", "r") as f:
    keys = json.load(f)

reddit_keys = keys["keys"]["reddit"]['script']


def get_reddit_api():
    return praw.Reddit(client_id=reddit_keys['client_id'],
                     client_secret=reddit_keys['secret_key'],
                     user_agent=reddit_keys['name'])


def dump_records(file, data):
    file_name = f"data/reddit_submissions/{file}.json"
    with open(file_name, 'w') as fp:
        json.dump(data, fp)
    return file_name


def get_score(post_id, reddit):
    try:
        submission = reddit.submission(id=post_id)
    except Exception:
        sys.exit(0)
        #with open('error_ids.txt', 'w+') as f:
        #    f.writeline(post_id + "\n")
        #    return False
    return submission.score

def check_files(a, b):
    with open(a, "r") as f:
        data_a = json.load(f)
    data_a = len(data_a['data'])
    with open(b, "r") as f:
        data_b = json.load(f)
    data_b = len(data_b['data'])
    if data_a == data_b:
        return True
    return False
    
    
file_list = glob.glob('data/reddit_submissions/*_v01.json')
for x in file_list:
    print(x)
    reddit = get_reddit_api()
    data = None
    with open(x, 'r') as f:
        data = json.load(f)
    if data:
        results = {'data': []}
        for post in data['data']:
            doc = post
            score = get_score(post['id'], reddit)
            if score:
                doc['score'] = score
            results['data'].append(doc)
        date = x.split('/')[-1].split('.')[0].split('_v01')[0]
        new_file = dump_records(date, results)
        if check_files(x, new_file):
            os.remove(x)
            print(f"File Updated: {new_file}")
            print(f"            Deleted: {x}")
sys.exit(1)
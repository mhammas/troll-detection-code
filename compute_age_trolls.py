import json
from datetime import datetime, date
import csv

all_comment_data = open('/data/hammas/reddit_troll_comments.txt')
user_dict = {}

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']	
    created_utc = post['created_utc']
    if author not in user_dict:
        user_dict[author] = {"age": 99999999999999}

    post_time = date.fromtimestamp(int(created_utc))
    post_year = post_time.year
    account_age = 2020 - post_year
    if account_age < user_dict[author]['age']:
        user_dict[author]['age'] = account_age


all_submission_data = open('/data/hammas/reddit_troll_submissions.txt')

for line in all_submission_data:
    post = json.loads(line)
    author = post['author']
    created_utc = post['created_utc']
    if author not in user_dict:
        user_dict[author] = {"age": 99999999999999}

    post_time = date.fromtimestamp(int(created_utc))
    post_year = post_time.year
    account_age = 2020 - post_year
    if account_age < user_dict[author]['age']:
        user_dict[author]['age'] = account_age

OUTPUT_PATH = '/data/hammas/features/'

with open(OUTPUT_PATH + 'trolls_age.csv', 'w', newline='') as csvfile:
    fieldnames = ['user', 
                'age']

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for user in user_dict.keys():
        writer.writerow({"user": user, 
                        "age": user_dict[user]["age"]})

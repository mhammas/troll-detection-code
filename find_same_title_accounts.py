import os
import json
import multiprocessing as mp
import numpy
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import STOPWORDS
import pandas as pd
import re

all_comment_data = open('/data/hammas/reddit_troll_comments.txt')
all_sub_data = open('/data/hammas/reddit_troll_submissions.txt')
troll_user_dict = {}
troll_titles = {}
suspicious_user_dict = {}
troll_titles_subreddits = {}

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']
    troll_user_dict[author] = 1

for line in all_sub_data:
    post = json.loads(line)
    author = post['author']
    troll_user_dict[author] = 1
    post_title = post['title']
    if post_title not in troll_titles:
    	troll_titles[post_title] = 0
    	troll_titles_subreddits[post_title] = set()
    troll_titles[post_title] += 1
    troll_titles_subreddits[post_title].add(post['selftext'])

DIR_PATH = '/data/spirits-backup/reddit/submissions/'
all_files = []

for file in os.listdir(DIR_PATH):
    if 'RS' in file:
        all_files.append(file)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

all_files_split = list(chunks(all_files, 22))

def my_func(proc_number): 
    file_division = all_files_split[proc_number]
    for file in file_division:
        f1 = open('/data/hammas/same_title_users_again/shortlisted_user_submissions_' + str(proc_number) + '.json','w')  
        print('File Number: ', file)
        f = open(DIR_PATH + file, 'r')
        for line in f:
            try:
                json_line = json.loads(line)
                author = json_line['author']
                if author not in troll_user_dict:
                    if json_line['title'] in troll_titles and json_line['selftext'] in troll_titles_subreddits[json_line['title']]:
                        json.dump(json_line, f1)
                        f1.write('\n')
            except:
                continue

pool = mp.Pool(mp.cpu_count())
result = pool.map(my_func, [0,1,2,3,4,5,6,7])
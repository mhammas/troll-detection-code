import os
import json
import multiprocessing as mp
import numpy

all_link_ids = {}

all_comment_data = open('/data/hammas/reddit_troll_submissions.txt')

for line in all_comment_data:
    post = json.loads(line)
    link_id = str('t3_' + post['id'])
    all_link_ids[link_id] = 1


DIR_PATH = '/data/spirits-backup/reddit/comments/'
all_files = []

for file in os.listdir(DIR_PATH):
    if 'RC' in file:
        all_files.append(file)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

all_files_split = list(chunks(all_files, 21))

def my_func(proc_number): 
    f1 = open('/data/hammas/filtered_comments_linkid_troll_posts/shortlisted_user_comments_' + str(proc_number) + '.json', 'w')
    file_division = all_files_split[proc_number]
    for file in file_division:  
        print('File Number: ', file)
        f = open(DIR_PATH + file, 'r')
        for line in f:
            json_line = json.loads(line)
            link_id = json_line['link_id']
            if link_id in all_link_ids:
                json.dump(json_line, f1)
                f1.write('\n')

pool = mp.Pool(mp.cpu_count())
result = pool.map(my_func, [0,1,2,3,4,5,6,7])
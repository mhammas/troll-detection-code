import os
import json
import multiprocessing as mp
import numpy

all_comment_data = open('/data/hammas/reddit_troll_comments.txt')
all_sub_data = open('/data/hammas/reddit_troll_submissions.txt')
troll_user_dict = {}

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']
    troll_user_dict[author] = 1

for line in all_sub_data:
    post = json.loads(line)
    author = post['author']
    troll_user_dict[author] = 1

print('Loading Comments')
id_to_post_dict = {}
user_list = {}
for i in range(0, 8):
    f = open('/data/hammas/filtered_comments_linkid_troll_posts/shortlisted_user_comments_' + str(i) + '.json', 'r')
    print('File Number: ', str(i))
    for line in f:
        json_line = json.loads(line)
        post_author = json_line['author']
        if post_author not in troll_user_dict:
            user_list[post_author] = 1

print('Length of User List: ', str(len(user_list)))

del user_list['[deleted]']

print('Starting Work')

DIR_PATH = '/data/spirits-backup/reddit/comments/'
#DIR_PATH = '/data/spirits-backup/reddit/submissions/'
all_files = []

for file in os.listdir(DIR_PATH):
    if 'RC' in file:
    #if 'RS' in file:
        all_files.append(file)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

all_files_split = list(chunks(all_files, 22))

def my_func(proc_number): 
    f1 = open('/data/hammas/filtered_comments_linkid_troll_posts_all_comments/shortlisted_user_comments_' + str(proc_number) + '.json', 'w')
    #f1 = open('/data/hammas/filtered_comments_linkid_troll_posts_all_submissions/shortlisted_user_submissions_' + str(proc_number) + '.json', 'w')
    file_division = all_files_split[proc_number]
    for file in file_division:  
        print('File Number: ', file)
        f = open(DIR_PATH + file, 'r')
        for line in f:
            json_line = json.loads(line)
            try:
                author = json_line['author']
                if author in user_list:
                    json.dump(json_line, f1)
                    f1.write('\n')
            except:
                continue

pool = mp.Pool(mp.cpu_count())
result = pool.map(my_func, [0,1,2,3,4,5,6,7])
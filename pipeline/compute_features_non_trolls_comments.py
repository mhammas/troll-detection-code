import os
import json
import multiprocessing as mp
import numpy
from multiprocessing import Process, Lock
import csv

posts_on_which_trolls_commented = {}
troll_user_dict = {}
all_comment_data = open('/data/hammas/reddit_troll_comments.txt')

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']	
    post_id = post['link_id']
    posts_on_which_trolls_commented[post_id] = 1
    troll_user_dict[author] = 1


posts_which_trolls_started = {}
all_submission_data = open('/data/hammas/reddit_troll_submissions.txt')

for line in all_submission_data:
    post = json.loads(line)
    author = post['author']
    link_id = str('t3_' + post['id'])
    posts_which_trolls_started[link_id] = 1
    troll_user_dict[author] = 1

#DIR_PATH = '/data/hammas/filtered_comments_again/'
#DIR_PATH = '/data/hammas/filtered_comments_title_users/'
#DIR_PATH = '/data/hammas/filtered_comments_linkid_troll_posts_all_comments/'
#DIR_PATH = '/data/hammas/missing_users/'
#DIR_PATH = '/data/hammas/random_accounts/'
DIR_PATH = '/data/hammas/same_title_users_data/'


all_files = []
for file in os.listdir(DIR_PATH):
	if 'comments' in file:
		all_files.append(file)

print("COMMENT ANALYSIS", DIR_PATH)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

all_files_split = list(chunks(all_files, 1))

def my_func(proc_number):
    file_division = all_files_split[proc_number]
    user_dict = {}
    for file_name in file_division:
        print(file_name)
        f = open(DIR_PATH + file_name, 'r')

        for line in f:
            json_line = json.loads(line)
            author = json_line['author']
            post_id = json_line['link_id']
            parent_id = json_line['parent_id']

            if author not in user_dict:
                user_dict[author] = {"comments_on_posts_that_trolls_commented": 0, 
                                    "comments_on_posts_that_trolls_started": 0,
                                    "direct_comment_reply_to_troll_post": 0,
                                    "total_comments": 0}

            if post_id in posts_on_which_trolls_commented:
                user_dict[author]["comments_on_posts_that_trolls_commented"] += 1

            if post_id in posts_which_trolls_started:
                user_dict[author]["comments_on_posts_that_trolls_started"] += 1

            if post_id in posts_which_trolls_started and parent_id == post_id:
                user_dict[author]["direct_comment_reply_to_troll_post"] += 1

            user_dict[author]['total_comments'] += 1

    OUTPUT_PATH = '/data/hammas/features/comments/'
    output_file_name = (DIR_PATH.split('/')[3])

    with open(OUTPUT_PATH + output_file_name + '_' + str(proc_number) +'.csv', 'w', newline='') as csvfile:
	    fieldnames = ['user', 
	    			'comments_on_posts_that_trolls_commented',
					'comments_on_posts_that_trolls_started', 
	    			'direct_comment_reply_to_troll_post',
					'total_comments']

	    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	    writer.writeheader()

	    for user in user_dict.keys():
	        writer.writerow({"user": user, 
							"comments_on_posts_that_trolls_commented": user_dict[user]["comments_on_posts_that_trolls_commented"],
							"comments_on_posts_that_trolls_started": user_dict[user]["comments_on_posts_that_trolls_started"],
							"direct_comment_reply_to_troll_post": user_dict[user]["direct_comment_reply_to_troll_post"],
							"total_comments": user_dict[user]["total_comments"]})


pool = mp.Pool(8)
result = pool.map(my_func, [0,1,2,3,4,5,6,7])

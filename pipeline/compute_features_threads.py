import json
import os
import json
import multiprocessing as mp
import numpy
from multiprocessing import Process, Lock
import csv

troll_user_dict = {}
all_comment_data = open('/data/hammas/reddit_troll_comments.txt')

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']	
    troll_user_dict[author] = 1


posts_which_trolls_started = {}
all_submission_data = open('/data/hammas/reddit_troll_submissions.txt')
troll_title_dict = {}

for line in all_submission_data:
    post = json.loads(line)
    author = post['author']
    link_id = str('t3_' + post['id'])
    posts_which_trolls_started[link_id] = 1
    troll_user_dict[author] = 1

user_dict = {}
print("THREAD ANALYSIS")

def myprint1(d):
	global user_dict
	for k, v in d.items():
		if isinstance(v, dict):
			myprint1(v)
			current_post_object = id_to_post_dict[k]
			current_post_author = current_post_object['author']
			current_post_object_post_id = current_post_object['link_id']

			if current_post_author not in troll_user_dict:
				for immediate_child_post in v:
					child_post_object = id_to_post_dict[immediate_child_post]
					child_post_author = child_post_object['author']

					if child_post_author in troll_user_dict:
						if current_post_author not in user_dict:
							user_dict[current_post_author] = {"thread_comment_reply_to_troll_comments": 0, "thread_comment_reply_to_troll_comments_in_troll_post": 0}
						user_dict[current_post_author]["thread_comment_reply_to_troll_comments"] += 1
						if current_post_object_post_id in posts_which_trolls_started:
							user_dict[current_post_author]["thread_comment_reply_to_troll_comments_in_troll_post"] += 1

			else:
				for immediate_child_post in v:
					child_post_object = id_to_post_dict[immediate_child_post]
					child_post_author = child_post_object['author']

					if child_post_author not in troll_user_dict:
						if child_post_author not in user_dict:
							user_dict[child_post_author] = {"thread_comment_reply_to_troll_comments": 0, "thread_comment_reply_to_troll_comments_in_troll_post": 0}
						user_dict[child_post_author]["thread_comment_reply_to_troll_comments"] += 1
						if current_post_object_post_id in posts_which_trolls_started:
							user_dict[child_post_author]["thread_comment_reply_to_troll_comments_in_troll_post"] += 1


f = open('arranged_threads1.json', 'r')
arranged_threads = json.load(f)

id_to_post_dict = {}
for i in range(0, 8):
	f = open('/data/hammas/filtered_comments_linkid/shortlisted_user_comments_' + str(i) + '.json', 'r')
	print('File Number: ', str(i))
	for line in f:
		json_line = json.loads(line)
		post_id = json_line['id']
		if post_id in id_to_post_dict:
			print('Duplicate Comment Found')
		else:
			id_to_post_dict[post_id] = json_line

myprint1(arranged_threads)


OUTPUT_PATH = '/data/hammas/features/'


print("SAVING DATA")

with open(OUTPUT_PATH + 'threads.csv', 'w', newline='') as csvfile:
    fieldnames = ['user', 
				'thread_comment_reply_to_troll_comments',
				'thread_comment_reply_to_troll_comments_in_troll_post']

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for user in user_dict.keys():
        writer.writerow({"user": user, 
						"thread_comment_reply_to_troll_comments": user_dict[user]["thread_comment_reply_to_troll_comments"],
						"thread_comment_reply_to_troll_comments_in_troll_post": user_dict[user]["thread_comment_reply_to_troll_comments_in_troll_post"]})
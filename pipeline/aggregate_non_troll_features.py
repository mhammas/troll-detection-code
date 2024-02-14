import os
import csv
import pandas as pd
import json

DIR_PATH = '/data/hammas/features/comments/'

troll_same_post_files = []
troll_post_reply_files = []

for file in os.listdir(DIR_PATH):
	if 'troll_posts' in file:
		troll_post_reply_files.append(file)
		print(file)
	if 'title' in file:
		troll_same_post_files.append(file)
		print(file)

user_dict = {}
set1 = {}
set2 = {}

for f in troll_post_reply_files:
	csvfile = open(DIR_PATH + f)
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		author = row[0]
		if row[1] != 'comments_on_posts_that_trolls_commented':
			if author not in user_dict:
				user_dict[author] = {"comments_on_posts_that_trolls_commented": int(row[1]), 
		                            "comments_on_posts_that_trolls_started": int(row[2]),
		                            "direct_comment_reply_to_troll_post": int(row[3]),
		                            "thread_comment_reply_to_troll_comments": 0,
		                            "thread_comment_reply_to_troll_comments_in_troll_post": 0,
		                            "same_title_post_as_troll": 0,
		                            "total_comments": int(row[4]),
		                            "total_submissions": 0}
			else:
				user_dict[author]["comments_on_posts_that_trolls_commented"]+=int(row[1])
				user_dict[author]["comments_on_posts_that_trolls_started"]+=int(row[2])
				user_dict[author]["direct_comment_reply_to_troll_post"]+=int(row[3])
				user_dict[author]["total_comments"]+=int(row[4])

			set1[author] = 1


for f in troll_same_post_files:
	csvfile = open(DIR_PATH + f)
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		author = row[0]
		if row[1] != 'comments_on_posts_that_trolls_commented':
			if author not in set1:
				if author not in user_dict:
					user_dict[author] = {"comments_on_posts_that_trolls_commented": int(row[1]), 
		                            "comments_on_posts_that_trolls_started": int(row[2]),
		                            "direct_comment_reply_to_troll_post": int(row[3]),
		                            "thread_comment_reply_to_troll_comments": 0,
		                            "thread_comment_reply_to_troll_comments_in_troll_post": 0,
		                            "same_title_post_as_troll": 0,
		                            "total_comments": int(row[4]),
		                            "total_submissions": 0}
				else:
					user_dict[author]["comments_on_posts_that_trolls_commented"]+=int(row[1])
					user_dict[author]["comments_on_posts_that_trolls_started"]+=int(row[2])
					user_dict[author]["direct_comment_reply_to_troll_post"]+=int(row[3])
					user_dict[author]["total_comments"]+=int(row[4])

				set2[author] = 1

DIR_PATH = '/data/hammas/features/submissions/'

troll_same_post_files = []
troll_post_reply_files = []

for file in os.listdir(DIR_PATH):
	if 'troll_posts' in file:
		troll_post_reply_files.append(file)
		print(file)
	if 'title' in file:
		troll_same_post_files.append(file)
		print(file)

set1 = {}
set2 = {}
user_dict1 = {}
for f in troll_post_reply_files:
	csvfile = open(DIR_PATH + f)
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		author = row[0]
		if row[1] != 'same_title_post_as_troll':
			if author not in user_dict1:
				user_dict1[author] = {"comments_on_posts_that_trolls_commented": 0, 
	                            "comments_on_posts_that_trolls_started": 0,
	                            "direct_comment_reply_to_troll_post": 0,
	                            "thread_comment_reply_to_troll_comments": 0,
	                            "thread_comment_reply_to_troll_comments_in_troll_post": 0,
	                            "same_title_post_as_troll": int(row[1]),
	                            "total_comments": 0,
	                            "total_submissions": int(row[2])}
			else:
				user_dict1[author]["same_title_post_as_troll"]+=int(row[1])
				user_dict1[author]["total_submissions"]+=int(row[2])
			
			set1[author] = 1


for f in troll_same_post_files:
	csvfile = open(DIR_PATH + f)
	readCSV = csv.reader(csvfile, delimiter=',')
	for row in readCSV:
		author = row[0]
		if row[1] != 'same_title_post_as_troll':
			if author not in set1:
				if author not in user_dict1:
					user_dict1[author] = {"comments_on_posts_that_trolls_commented": 0, 
		                            "comments_on_posts_that_trolls_started": 0,
		                            "direct_comment_reply_to_troll_post": 0,
		                            "thread_comment_reply_to_troll_comments": 0,
		                            "thread_comment_reply_to_troll_comments_in_troll_post": 0,
		                            "same_title_post_as_troll": int(row[1]),
		                            "total_comments": 0,
		                            "total_submissions": int(row[2])}
				else:
					user_dict1[author]["same_title_post_as_troll"]+=int(row[1])
					user_dict1[author]["total_submissions"]+=int(row[2])
				set2[author] = 1

for author in user_dict:
	if author in user_dict1:
		user_dict[author]["same_title_post_as_troll"] = user_dict1[author]["same_title_post_as_troll"] 
		user_dict[author]["total_submissions"] = user_dict1[author]["total_submissions"] 
		del user_dict1[author]

count = 0

for author in user_dict1:
	if author not in user_dict:
		user_dict[author] = {"comments_on_posts_that_trolls_commented": 0, 
	                    "comments_on_posts_that_trolls_started": 0,
	                    "direct_comment_reply_to_troll_post": 0,
	                    "thread_comment_reply_to_troll_comments": 0,
	                    "thread_comment_reply_to_troll_comments_in_troll_post": 0,
	                    "same_title_post_as_troll": int(user_dict1[author]["same_title_post_as_troll"]),
	                    "total_comments": 0,
	                    "total_submissions": int(user_dict1[author]["total_submissions"])}


csvfile = open('/data/hammas/features/threads.csv')

readCSV = csv.reader(csvfile, delimiter=',')
for row in readCSV:
	author = row[0]
	if row[1] != 'thread_comment_reply_to_troll_comments' and author != "[deleted]":
		if author in user_dict:
			user_dict[author]["thread_comment_reply_to_troll_comments"]+=int(row[1])
			user_dict[author]["thread_comment_reply_to_troll_comments_in_troll_post"]+=int(row[2])

with open('/data/hammas/features/non_trolls_new.csv', 'w', newline='') as csvfile:
    fieldnames = ["user",
                "comments_on_posts_that_trolls_commented", 
                 "comments_on_posts_that_trolls_started",
                "direct_comment_reply_to_troll_post",
                "thread_comment_reply_to_troll_comments",
                "thread_comment_reply_to_troll_comments_in_troll_post",
                "same_title_post_as_troll",
                "total_comments",
                "total_submissions"]

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for user in user_dict.keys():
        writer.writerow({"user": user, 
        				"comments_on_posts_that_trolls_commented": user_dict[user]["comments_on_posts_that_trolls_commented"], 
		                "comments_on_posts_that_trolls_started": user_dict[user]["comments_on_posts_that_trolls_started"],
		                "direct_comment_reply_to_troll_post": user_dict[user]["direct_comment_reply_to_troll_post"],
		                "thread_comment_reply_to_troll_comments": user_dict[user]["thread_comment_reply_to_troll_comments"],
		                "thread_comment_reply_to_troll_comments_in_troll_post": user_dict[user]["thread_comment_reply_to_troll_comments_in_troll_post"],
		                "same_title_post_as_troll": user_dict[user]["same_title_post_as_troll"],
		                "total_comments": user_dict[user]["total_comments"],
		                "total_submissions": user_dict[user]["total_submissions"]})
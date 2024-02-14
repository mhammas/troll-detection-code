import os
import json
import multiprocessing as mp
import numpy
from multiprocessing import Process, Lock
import csv
import pandas as pd
import copy


user_dict = {}
post_commenters_dict = {}
post_started_dict = {}
title_posters_dict = {}
post_commented_list = []
post_started_list = []
posts_on_which_trolls_commented = {}
troll_user_dict = {}
all_comment_data = open('/data/hammas/reddit_troll_comments.txt')

for line in all_comment_data:
    post = json.loads(line)
    author = post['author']	
    post_id = post['link_id']
    posts_on_which_trolls_commented[post_id] = 1
    troll_user_dict[author] = 1
    post_commented_list.append([post_id, author])

    if post_id not in post_commenters_dict:
        post_commenters_dict[post_id] = []
    post_commenters_dict[post_id].append(author)

    if author not in user_dict:
        user_dict[author] = {"comments_on_posts_that_trolls_commented": 0, 
                        "comments_on_posts_that_trolls_started": 0,
                        "direct_comment_reply_to_troll_post": 0,
                        "thread_comment_reply_to_troll_comments": 0,
                        "thread_comment_reply_to_troll_comments_in_troll_post": 0,
                        "same_title_post_as_troll": 0,
                        "total_comments": 0,
                        "total_submissions": 0}


posts_which_trolls_started = {}
all_submission_data = open('/data/hammas/reddit_troll_submissions.txt')
troll_title_dict = {}

for line in all_submission_data:
    post = json.loads(line)
    author = post['author']
    link_id = str('t3_' + post['id'])
    title = post['title']
    troll_title_dict[title] = 1
    posts_which_trolls_started[link_id] = 1
    troll_user_dict[author] = 1

    post_started_dict[link_id] = author

    if title not in title_posters_dict:
        title_posters_dict[title] = []
    title_posters_dict[title].append(author)
    
    if author not in user_dict:
        user_dict[author] = {"comments_on_posts_that_trolls_commented": 0, 
                        "comments_on_posts_that_trolls_started": 0,
                        "direct_comment_reply_to_troll_post": 0,
                        "thread_comment_reply_to_troll_comments": 0,
                        "thread_comment_reply_to_troll_comments_in_troll_post": 0,
                        "same_title_post_as_troll": 0,
                        "total_comments": 0,
                        "total_submissions": 0}

f = open('/data/hammas/reddit_troll_comments.txt')
for line in f:
    json_line = json.loads(line)
    author = json_line['author']
    post_id = json_line['link_id']
    parent_id = json_line['parent_id']  

    comments_on_post = copy.deepcopy(post_commenters_dict[post_id])

    if len(list(filter(lambda a: a != author, comments_on_post))) > 0:
        user_dict[author]["comments_on_posts_that_trolls_commented"] += 1

    if post_id in posts_which_trolls_started and post_started_dict[post_id] != author:
        user_dict[author]["comments_on_posts_that_trolls_started"] += 1

    if post_id in posts_which_trolls_started and parent_id == post_id and post_started_dict[post_id] != author:
        user_dict[author]["direct_comment_reply_to_troll_post"] += 1

    user_dict[author]['total_comments'] += 1

f = open('/data/hammas/reddit_troll_submissions.txt')
for line in f:
    json_line = json.loads(line)
    author = json_line['author']
    title = json_line['title'] 

    same_title_posters = copy.deepcopy(title_posters_dict[title])

    if len(list(filter(lambda a: a != author, same_title_posters))) > 0:
        user_dict[author]["same_title_post_as_troll"] += 1

    user_dict[author]['total_submissions'] += 1

def myprint1(d):
    global user_dict
    for k, v in d.items():
        if isinstance(v, dict):
            myprint1(v)
            current_post_object = id_to_post_dict[k]
            current_post_author = current_post_object['author']
            current_post_object_post_id = current_post_object['link_id']

            if current_post_author in troll_user_dict:
                for immediate_child_post in v:
                    child_post_object = id_to_post_dict[immediate_child_post]
                    child_post_author = child_post_object['author']

                    if child_post_author in troll_user_dict and current_post_author != "[deleted]" and current_post_author != child_post_author:
                        user_dict[current_post_author]["thread_comment_reply_to_troll_comments"] += 1
                        if current_post_object_post_id in posts_which_trolls_started:
                            user_dict[current_post_author]["thread_comment_reply_to_troll_comments_in_troll_post"] += 1



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

with open('/data/hammas/features/trolls_again_again.csv', 'w', newline='') as csvfile:
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
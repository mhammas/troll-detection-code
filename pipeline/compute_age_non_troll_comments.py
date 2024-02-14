import os
import json
import multiprocessing as mp
import numpy
from multiprocessing import Process, Lock
import csv
from datetime import datetime, date

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
            created_utc = json_line['created_utc']
            if author not in user_dict:
                user_dict[author] = {"age": 99999999999999}

            post_time = date.fromtimestamp(int(created_utc))
            post_year = post_time.year
            account_age = 2020 - post_year
            if account_age < user_dict[author]['age']:
                user_dict[author]['age'] = account_age

    OUTPUT_PATH = '/data/hammas/features/age/comments/'
    output_file_name = (DIR_PATH.split('/')[3])

    with open(OUTPUT_PATH + output_file_name + '_' + str(proc_number) +'.csv', 'w', newline='') as csvfile:
	    fieldnames = ['user', 
					'age']

	    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	    writer.writeheader()

	    for user in user_dict.keys():
	        writer.writerow({"user": user, 
							"age": user_dict[user]["age"]})


pool = mp.Pool(8)
result = pool.map(my_func, [0,1,2,3,4,5,6,7])

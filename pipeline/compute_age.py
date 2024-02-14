import os
import csv
COMMENT_DIR_PATH = '/data/hammas/features/age/comments/'
SUBMISSION_DIR_PATH = '/data/hammas/features/age/submissions/'

user_dict = {}
for file in os.listdir(COMMENT_DIR_PATH):
    try:
        print(file)
        f = open(COMMENT_DIR_PATH + file, newline='')
        reader = csv.reader(f, delimiter=' ', quotechar='|')
        first = True
        for line in reader:
            if first == True:
                first = False
            else:
                author = line[0].split(',')[0]
                age = int(line[0].split(',')[1])
                if author not in user_dict:
                    user_dict[author] = age
                else:
                	if age > user_dict[author]:
                		user_dict[author] = age
    except:
        print("ERROR: ", file)


for file in os.listdir(SUBMISSION_DIR_PATH):
    try:
        print(file)
        f = open(SUBMISSION_DIR_PATH + file, newline='')
        reader = csv.reader(f, delimiter=' ', quotechar='|')
        first = True
        for line in reader:
            if first == True:
                first = False
            else:
                author = line[0].split(',')[0]
                age = int(line[0].split(',')[1])
                if author not in user_dict:
                    user_dict[author] = age
                else:
                	if age > user_dict[author]:
                		user_dict[author] = age
    except:
        print("ERROR: ", file)

OUTPUT_PATH = '/data/hammas/features/'

with open(OUTPUT_PATH + 'non_trolls_age.csv', 'w', newline='') as csvfile:
    fieldnames = ['user', 
                'age']

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for user in user_dict.keys():
        writer.writerow({"user": user, 
                        "age": user_dict[user]})
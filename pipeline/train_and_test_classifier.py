from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.svm import SVC
from sklearn.metrics import roc_curve, auc, roc_auc_score

import pandas as pd
import numpy as np
import json
import math
import csv
import random


#INITIALIZING VARIABLES

features = []
labels = []
users = []

selected_random_users = open('/data/hammas/selected_random_users_for_machine_learning.txt', 'r')
troll_user_dict = {}
suspected_troll_dict = {}
selected_random_users_dict = {}

for line in selected_random_users:
    selected_random_users_dict[line[:-1]] = 1



#LOADING FEATURES FOR TROLLS


for feature_no in range(1,7):
    f = open('/data/hammas/features/trolls_again_again.csv', newline='')
    reader = csv.reader(f, delimiter=' ', quotechar='|')
    first = True
    for line in reader:
        if first == True:
            feature_name = line[0].split(',')[feature_no]
            if feature_no == 6:
                divisor = 8
            else:
                divisor = 7
            first = False
        else:
            author = line[0].split(',')[0]
            feature_result = int(line[0].split(',')[feature_no])
            total = int(line[0].split(',')[divisor])
            if author not in troll_user_dict:
                troll_user_dict[author] = {}
            troll_user_dict[author]['total_comments'] = int(line[0].split(',')[7])
            troll_user_dict[author]['total_submissions'] = int(line[0].split(',')[8])
            if feature_result != 0:
                feature_result = float(feature_result/total)
            else:
                feature_result = float(feature_result) 
            troll_user_dict[author][feature_name] = feature_result

f = open('/data/hammas/features/trolls_age.csv', newline='')
reader = csv.reader(f, delimiter=' ', quotechar='|')
first = True
for line in reader:
    if first == True:
        first = False
    else:
        author = line[0].split(',')[0]
        age = line[0].split(',')[1]
        troll_user_dict[author]['age'] = int(age)

#LOADING FEATURES FOR NON-TROLLS

for feature_no in range(1,7):
    f = open('/data/hammas/features/non_trolls_new.csv', newline='')
    reader = csv.reader(f, delimiter=' ', quotechar='|')
    first = True
    for line in reader:
        if first == True:
            feature_name = line[0].split(',')[feature_no]
            if feature_no == 6:
                divisor = 8
            else:
                divisor = 7
            first = False
        else:
            author = line[0].split(',')[0]
            if author in selected_random_users_dict:
                feature_result = int(line[0].split(',')[feature_no])
                total = int(line[0].split(',')[divisor])
                if author not in suspected_troll_dict:
                    suspected_troll_dict[author] = {}
                suspected_troll_dict[author]['total_comments'] = int(line[0].split(',')[7])
                suspected_troll_dict[author]['total_submissions'] = int(line[0].split(',')[8])
                if feature_result != 0:
                    feature_result = float(feature_result/total)
                else:
                    feature_result = float(feature_result)   
                suspected_troll_dict[author][feature_name] = feature_result


f = open('/data/hammas/features/non_trolls_age.csv', newline='')
reader = csv.reader(f, delimiter=' ', quotechar='|')
first = True
for line in reader:
    if first == True:
        first = False
    else:
        author = line[0].split(',')[0]
        age = line[0].split(',')[1]
        if author in suspected_troll_dict:
            suspected_troll_dict[author]['age'] = int(age)


print(len(suspected_troll_dict))
print(len(troll_user_dict))


#PREPARING DATA FOR CLASSIFIER

features_dict = {}
for user in troll_user_dict:
    user_arr = []
    for val in troll_user_dict[user]:
        features_dict[val] = 1
        user_arr.append(troll_user_dict[user][val])
    features.append(user_arr)
    labels.append(1)
    users.append(user)

for user in suspected_troll_dict:
    user_arr = []
    for val in suspected_troll_dict[user]:
        user_arr.append(suspected_troll_dict[user][val])
    features.append(user_arr)
    labels.append(0)
    users.append(user)


#RUNNING THE CLASSIFIER

#classifier = KNeighborsClassifier()
#classifier = DecisionTreeClassifier()
classifier = RandomForestClassifier()
#classifier = SVC(kernel='linear')

trainScores = []
trainPrec = []
trainRecall = []
testScores = []
testPrec = []
testRecall = []
rocAUCSscore = []
for q in range(0, 10):
    
    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size = 0.2)
    classifier.fit(X_train, y_train)

    predictionsTest = classifier.predict(X_test)
    predictionsTrain = classifier.predict(X_train)

    scoreTest = classifier.score(X_test, y_test)
    scoreTrain = classifier.score(X_train, y_train)
    precTrain = precision_score(y_train, predictionsTrain, average='macro', zero_division = 0)
    recallTrain = recall_score(y_train, predictionsTrain, average='macro', zero_division = 0)
    precTest = precision_score(y_test, predictionsTest, average='macro', zero_division = 0)
    recallTest = recall_score(y_test, predictionsTest, average='macro', zero_division = 0)

    trainScores.append(scoreTrain)
    trainPrec.append(precTrain)
    trainRecall.append(recallTrain)
    testScores.append(scoreTest)
    testPrec.append(precTest)
    testRecall.append(recallTest)
    rocAUCSscore.append(roc_auc_score(y_test, predictionsTest))
    print('-- Individual test and train accuracy', scoreTest, scoreTrain)

trainScores = np.mean(trainScores)
trainPrec = np.mean(trainPrec)
trainRecall = np.mean(trainRecall)
testScores = np.mean(testScores)
testPrec = np.mean(testPrec)
testRecall = np.mean(testRecall)
rocAUCSscore = np.mean(rocAUCSscore) 
print('-- Final test and train accuracy', testScores, trainScores)

print("precision_score")
print(testPrec)
print("recall_score")
print(testRecall)
print("AUC Score")
print(rocAUCSscore)


#LOAD TEST USER DATA

test_user_dict = {}


for feature_no in range(1,7):
    f = open('/data/hammas/features/non_trolls_new.csv', newline='')
    reader = csv.reader(f, delimiter=' ', quotechar='|')
    first = True
    for line in reader:
        if first == True:
            feature_name = line[0].split(',')[feature_no]
            if feature_no == 6:
                divisor = 8
            else:
                divisor = 7
            first = False
        else:
            author = line[0].split(',')[0]
            if author not in selected_random_users_dict:
                feature_result = int(line[0].split(',')[feature_no])
                total = int(line[0].split(',')[divisor])
                if author not in test_user_dict:
                    test_user_dict[author] = {}
                test_user_dict[author]['total_comments'] = int(line[0].split(',')[7])
                test_user_dict[author]['total_submissions'] = int(line[0].split(',')[8])
                if feature_result != 0:
                    feature_result = float(feature_result/total)
                else:
                    feature_result = float(feature_result)    
                test_user_dict[author][feature_name] = feature_result

f = open('/data/hammas/features/non_trolls_age.csv', newline='')
reader = csv.reader(f, delimiter=' ', quotechar='|')
first = True
for line in reader:
    if first == True:
        first = False
    else:
        author = line[0].split(',')[0]
        age = line[0].split(',')[1]
        if author in test_user_dict:
            test_user_dict[author]['age'] = int(age)



#USE MODEL TO PREDCIT

letsTest = []
letsTestUsers = []

for user in test_user_dict:
    user_arr = []
    for val in test_user_dict[user]:
        user_arr.append(test_user_dict[user][val])
    letsTest.append(user_arr)
    letsTestUsers.append(user)

myPredictions = classifier.predict(letsTest)
iteration = 0
pos = 0
neg = 0
users = []

for i in myPredictions:
    if i ==1:
        pos +=1
        users.append(letsTestUsers[iteration])
    else:
        neg += 1
    iteration += 1

print("Identified Trolls: ", pos)
print("Non-Trolls", neg)
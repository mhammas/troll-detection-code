import csv
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


# Function to read and preprocess the data
def read_and_preprocess_csv(filename):
    data = []
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            # Convert data to float and preprocess features
            user_id, feature1, feature2, feature3, feature4, feature5, feature6, total_comments, total_submission, age = map(float, row)
            if total_comments == 0:
                total_comments = 1  # To avoid division by zero
            if total_submission == 0:
                total_submission = 1  # To avoid division by zero
            feature1 /= total_comments
            feature2 /= total_comments
            feature3 /= total_comments
            feature4 /= total_comments
            feature5 /= total_comments
            feature6 /= total_submission
            data.append([feature1, feature2, feature3, feature4, feature5, feature6, total_comments, total_submission, age])
    return data

# Load and preprocess the data
csv1_data = read_and_preprocess_csv('trolls_data.csv')
csv2_data = read_and_preprocess_csv('non_trolls_data.csv')

# Create labels (positive and negative classes)
csv1_labels = np.ones(len(csv1_data), dtype=int)
csv2_labels = np.zeros(len(csv2_data), dtype=int)

# Combine the datasets and labels
X = np.vstack((csv1_data, csv2_data))
y = np.concatenate((csv1_labels, csv2_labels))

# Running the classifier
classifier = RandomForestClassifier()

trainScores = []
trainPrec = []
trainRecall = []
testScores = []
testPrec = []
testRecall = []

for q in range(0, 10):
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)
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
    print('-- Individual test and train accuracy', scoreTest, scoreTrain)

trainScores = np.mean(trainScores)
trainPrec = np.mean(trainPrec)
trainRecall = np.mean(trainRecall)
testScores = np.mean(testScores)
testPrec = np.mean(testPrec)
testRecall = np.mean(testRecall)
print('-- Final test and train accuracy', testScores, trainScores)

print("precision_score")
print(testPrec)
print("recall_score")
print(testRecall)

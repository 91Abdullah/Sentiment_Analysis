import sys
import shutil
import nltk
from pyspark import SparkConf, SparkContext
from nltk.tokenize import word_tokenize
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel


conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)

pos = sc.textFile("C:\\Spark\\pos.txt")
neg = sc.textFile("C:\\Spark\\neg.txt")
pos_sp = pos.flatMap(lambda line: line.split("\n")).collect()
neg_sp = neg.flatMap(lambda line: line.split("\n")).collect()

all_words = []
documents = []
allowed = ["J", "R", "V", "N"]

for p in pos_sp:
	documents.append({"text": p, "label": 1})

for p in neg_sp:
	documents.append({"text": p, "label": 0})


def wc(data):
	words = word_tokenize(data)
	tag = nltk.pos_tag(words)
	for w in tag:
		if w[1][0] in allowed:
			all_words.append(w[0].lower())
	return all_words


raw_data = sc.parallelize(documents)
raw_tokenized = raw_data.map(lambda dic: {"text": wc(dic["text"]), "label": dic["label"]})

htf = HashingTF(50000)
raw_hashed = raw_tokenized.map(lambda dic: LabeledPoint(dic["label"], htf.transform(dic["text"])))
raw_hashed.persist()

trained_hashed, test_hashed = raw_hashed.randomSplit([0.7, 0.3])

NB_model = NaiveBayes.train(trained_hashed)
NB_prediction_and_labels = test_hashed.map(lambda point: (NB_model.predict(point.features), point.label))
NB_correct = NB_prediction_and_labels.filter(lambda x: x[0] == x[1])
NB_accuracy = NB_correct.count() / float(test_hashed.count())
print("NB training accuracy:" + str(NB_accuracy * 100) + " %")
NB_output_dir = 'C:\\Spark\\NaiveBayes'
shutil.rmtree("C:\\Spark\\NaiveBayes\\metadata", ignore_errors=True)
NB_model.save(sc, NB_output_dir)

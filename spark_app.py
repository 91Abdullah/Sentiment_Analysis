from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import NaiveBayesModel
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

htf = HashingTF(50000)
NB_output_dir = 'C:\\Spark\\NaiveBayes'
NB_load_model = NaiveBayesModel.load(sc, NB_output_dir)

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    return SQLContext(spark_context)


def send_df_to_dashboard(df, analysis):
    print("------------------ %s -----------------" % analysis)
    if analysis == 'model':
        sentiment = [str(t.sentiment) for t in df.select("sentiment").collect()]
        frequency = [p.frequency for p in df.select("frequency").collect()]
        url = 'http://localhost:5001/updateSentiments'
        request_data = {'label': str(sentiment), 'data': str(frequency)}
    else:
        top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
        print("Top tags: %s" % str(top_tags))
        # print("DF: %s" % df.show())
        tags_count = [t.hashtag_count for t in df.select("hashtag_count").collect()]
        # print("Tags Count: %s" % str(tags_count))
        url = 'http://localhost:5001/updateData'
        request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)
    print('Data sent to server...')


def process_rdd(time, rdd):
    print("-------------- %s -----------------" % str(time))
    print("-------------- %s -----------------" % str(rdd.collect()))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        hashtags_df.registerTempTable("hashtags")
        hashtags_counts_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtags_counts_df.show()
        send_df_to_dashboard(hashtags_counts_df, 'hashtag')
    except:
        e = sys.exc_info()[1]
        s = sys.exc_info()[0]
        print("Error: %s" % e)
        print("Trace %s" % s)


def process_model_rdd(rdd):
    print('---- Starting RDD for model ----')
    print("-------------- %s --------------" % str(rdd.collect()))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # transformed_rdd = rdd.map(lambda x: 'positive' if x[1]==0 else '')
        row_rdd = rdd.map(lambda w: Row(sentiment=w[0], frequency=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # print(hashtags_df.collect())
        hashtags_df.registerTempTable("sentiments")
        hashtag_counts_df = sql_context.sql("select sentiment, frequency FROM sentiments")
        hashtag_counts_df.show()
        # print(hashtag_counts_df.count())
        send_df_to_dashboard(hashtag_counts_df, 'model')
    except:
        pass


def classify(transformer):
    v = NB_load_model.predict(transformer)
    return v


# Split each tweet as words
model_words = dataStream.flatMap(lambda line: line.split(" "))
features = model_words.map(lambda x: htf.transform(x))
prediction = features.map(lambda x: classify(x))
labelled_sentiments = prediction.map(lambda x: ('positive', 1) if x == 1 else ('negative', 1))
# results = labelled_sentiments.reduceByKey(lambda a, b: a+b)
results = labelled_sentiments.updateStateByKey(aggregate_tags_count)

# Split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# tags_totals = hashtags.reduceByKey(lambda a, b: a + b)

results.foreachRDD(lambda x: process_model_rdd(x))
tags_totals.foreachRDD(process_rdd)
# start
ssc.start()
ssc.awaitTermination()

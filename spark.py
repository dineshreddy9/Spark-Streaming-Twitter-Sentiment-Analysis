from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# citation: Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
import json
import re
from elasticsearch import Elasticsearch
import time

TCP_IP = 'localhost'
TCP_PORT = 9001

def sentimentExtract(text):
    senti = SentimentIntensityAnalyzer()
    polarity = senti.polarity_scores(text)
    if(polarity["compound"] > 0):
        return "positive"
    elif(polarity["compound"] < 0):
        return "negative"
    else:
        return "neutral"

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
dataStream.pprint()
es = Elasticsearch()
ts = time.time() 
tsmilli = ts*1000
es_conf = { "es.resource" : "tweet/tweetDetails", "es.nodes":"localhost", "es.port":"9200"} 
dstreamjson = dataStream.map(lambda line: json.loads(line)).map(lambda line: {"text": line["cleaned"], "sentiment": sentimentExtract(line["cleaned"]), "location":{"lat": line["loc"].split("::::")[0], "lon": line["loc"].split("::::")[1]}, "timestamp":tsmilli})
dstreamjson2 = dstreamjson.map(lambda x : ("a", x))
dstreamjson2.foreachRDD(lambda line : line.saveAsNewAPIHadoopFile(
            path="-", 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_conf))
            

######### your processing here ###################
dstreamjson2.pprint()

#################################################

ssc.start()
ssc.awaitTermination()

'''
PUT tweet
{
  "mappings": {
    "tweetDetails": {
      "properties": {
        "sentiment": {
          "type": "text"
        },
        "location": {
          "type": "geo_point"
        },
        "timestamp": {
          "type": "date",
          "format": "epoch_millis"
        },
        "text": {
          "type": "text",
          "fields":{
            "keyword": {
                "type" : "keyword"
            }
          }
        }
      }
    }
  }
}
'''

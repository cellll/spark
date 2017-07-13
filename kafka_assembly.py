import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime, timedelta, date
import time


def none_decoder(s):
	if s is None:
		return None
	return s

def getDirectStream(self):
	def iter(time, rdd):
		taken = rdd.take(1000)
		print("Time : %s" % time)

		for record in taken:
			key = record[0]
			value = record[1]

			print ("{} : {}".format(key, value))

	self.foreachRDD(iter)



def main():
	conf = SparkConf()
	conf.setMaster("spark://192.168.1.72:37077")
	conf.setAppName("test")
	sc = SparkContext(conf=conf)

	ssc = StreamingContext(sc, 1)
	topic=['s']
	kafkaParams = {"metadata.broker.list":"localhost:9092","auto.create.topics.enable":"true","auto.offset.reset":"smallest"}
	
	ds = KafkaUtils.createDirectStream(ssc, topic, kafkaParams, valueDecoder=none_decoder)

	#getDirectStream(ds)
	ds.pprint()

	ssc.start()
	ssc.awaitTermination()

main()

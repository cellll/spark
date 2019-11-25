import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from datetime import datetime, timedelta, date
import time
import gc
import configparser

importFile = None
sysPath = None
fcount = 0
CoreProcess = None

def parse_rdd(ds):
    global CoreProcess
    print ("::::::::::::::::::::::START::::::::::::::::::::::::::\n")
    
    CoreProcess = __import__(importFile.split("/")[len(importFile.split("/")) - 1].split(".")[0])
    ds.foreachRDD(lambda rdd: rdd.foreachPartition(parse_image))

    
def parse_image(iter):
    global importFile, sysPath, CoreProcess
    
    for record in iter:
        t1 = time.time()
        CoreProcess.proc(record, sysPath)
        t2 = time.time()
        print ('time :', t2-t1)
        print ('end for')
    if CoreProcess.isInference():
        print ('start Inference')
        t1 = time.time()
        CoreProcess.done()
        t2 = time.time()
        print ("DONE DURATION :: ", t2 - t1)
    

def none_decoder(s):
    return None if a is None else s

def main(InfoAppName="consumer", InfoTopic="chpart1", InfoAddPyFile="/home/test/CoreProcessAType__.py", InfoTotalCore=4, SysPath=""):
    global importFile, sysPath

    prop = configparser.RawConfigParser()
    prop.read('SparkConfig.properties')

    importFile = InfoAddPyFile

    sysPath = SysPath
    conf = SparkConf()
    conf.setMaster(prop.get('SparkConfig', 'spark.master'))
    conf.setAppName(InfoAppName)
    
    ##
    conf.set("spark.cores.max", InfoTotalCore)
    conf.set("spark.streaming.backpressure.enabled", prop.get(
        'SparkConfig', 'spark.streaming.backpressure.enabled'))
    conf.set("spark.executor.memory", prop.get(
        'SparkConfig', 'spark.executor.memory'))
    conf.set("spark.python.worker.memory", prop.get(
        'SparkConfig', 'spark.python.worker.memory'))
    conf.set("spark.streaming.concurrentJobs", prop.get(
        'SparkConfig', 'spark.streaming.concurrentJobs'))
    conf.set("spark.executor.cores", prop.get(
        'SparkConfig', 'spark.executor.cores'))
    conf.set("spark.task.cpus", prop.get('SparkConfig', 'spark.task.cpus'))
    conf.set("spark.executor.extraLibraryPath", prop.get(
        'SparkConfig', 'spark.executor.extraLibraryPath'))
    conf.set("spark.locality.wait", prop.get(
        'SparkConfig', 'spark.locality.wait'))
    conf.set("spark.scheduler.mode", prop.get(
        'SparkConfig', 'spark.scheduler.mode'))
    conf.set("spark.streaming.blockInterval", prop.get(
        'SparkConfig', 'spark.streaming.blockInterval'))
    conf.set("spark.serializer", prop.get('SparkConfig', 'spark.serializer'))
    
    
    kafkaParams = {"metadata.broker.list": prop.get(
        'KafkaConfig', 'metadata.broker.list'), "group.id": prop.get('KafkaConfig', 'group.id')}

    sc = SparkContext(conf=conf)
    sc.addPyFile(InfoAddPyFile)
    sc.addFile('c_count.txt')
    sc.addFile('p_count.txt')
    ssc = StreamingContext(sc, 1)
    topic1 = [InfoTopic]

    dstream = KafkaUtils.createDirectStream(
        ssc, topic1, kafkaParams, valueDecoder=none_decoder)
    
    parse_rdd(dstream)
    ssc.start()
    ssc.awaitTermination()


print (sys.argv)
main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])


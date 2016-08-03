import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from amqp import AMQPUtils

def getTemperature(jsonMsg):
    data = json.loads(jsonMsg)
    # temperature is inside the body as an amqp value
    return int(data["body"]["section"])

def getMax(a,b):
    if (a > b):
        return a
    else:
        return b

def createStreamingContext():
    conf = (SparkConf().setMaster("local[2]").setAppName("amqp_temperature"))
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/spark-streaming-amqp")

    receivestream = AMQPUtils.createStream(ssc, "localhost", 5672, "temperature")

    temperature = receivestream.map(getTemperature)
    max = temperature.reduceByWindow(getMax, None, 5, 5)

    max.pprint()

    ssc

ssc1 = StreamingContext.getOrCreate("/tmp/spark-streaming-amqp", createStreamingContext)
ssc1.start()
ssc1.awaitTermination()

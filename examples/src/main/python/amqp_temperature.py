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
    conf = SparkConf().setMaster("local[2]").setAppName("amqp_temperature")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    sc = SparkContext.getOrCreate(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/spark-streaming-amqp")

    receiveStream = AMQPUtils.createStream(ssc, "localhost", 5672, "username", "password", "temperature")

    temperature = receiveStream.map(getTemperature)
    max = temperature.reduceByWindow(getMax, None, 5, 5)

    max.pprint()

    return ssc

ssc = StreamingContext.getOrCreate("/tmp/spark-streaming-amqp", createStreamingContext)

ssc.start()
ssc.awaitTermination()

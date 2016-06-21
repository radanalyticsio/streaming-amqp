package com.redhat.spark.streaming.amqp

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel

object AMQPUtils {
  
  /**
   * Create an input stream that receives messages from an AMQP sender node
   * @param ssc Spark Streaming context
   * @param storageLevel RDD storage level
   */
  def createStream(
      ssc: StreamingContext,
      storageLevel: StorageLevel
      ) : ReceiverInputDStream[String] = {
    new AMQPInputDStream(ssc, storageLevel)
  }
}
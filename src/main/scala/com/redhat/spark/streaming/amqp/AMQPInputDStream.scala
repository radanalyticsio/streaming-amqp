package com.redhat.spark.streaming.amqp

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

/**
 * Input stream that receives messages from an AMQP sender node
 * @param ssc Spark Streaming context
 * @param storageLevel RDD storage level
 */
class AMQPInputDStream(
      ssc: StreamingContext,
      storageLevel: StorageLevel
    ) extends ReceiverInputDStream[String](ssc) {
  
  def getReceiver(): Receiver[String] = {
    new AMQPReceiver(storageLevel)
  }
}
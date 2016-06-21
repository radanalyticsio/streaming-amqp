package com.redhat.spark.streaming.amqp

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

/**
 * Receiver for getting messages from an AMQP sender node
 * @param storageLevel RDD storage level
 */
class AMQPReceiver(
      storageLevel: StorageLevel
    ) extends Receiver[String](storageLevel) {
  
  def onStart() {
    
  }
  
  def onStop() {
    
  }
}
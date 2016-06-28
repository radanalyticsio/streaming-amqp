/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.amqp

import org.apache.qpid.proton.message.Message
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.reflect.ClassTag

object AMQPUtils {
  
  /**
   * Create an input stream that receives messages from an AMQP sender node
   * @param ssc						    Spark Streaming context
   * @param host					    AMQP container hostname or IP address to connect
   * @param port					    AMQP container port to connect
   * @param address				    AMQP node address on which receive messages
   * @param messageConverter  Callback for converting AMQP message to custom type at application level
   * @param storageLevel      RDD storage level
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      host: String,
      port: Int,
      address: String,
      messageConverter: Message => Option[T],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[T] = {
    new AMQPInputDStream(ssc, host, port, address, messageConverter, storageLevel)
  }

  /**
   * Create an input stream that receives messages from an AMQP sender node
   * @param jssc						  Java Spark Streaming context
   * @param host					    AMQP container hostname or IP address to connect
   * @param port					    AMQP container port to connect
   * @param address				    AMQP node address on which receive messages
   * @param messageConverter  Callback for converting AMQP message to custom type at application level
   * @param storageLevel      RDD storage level
   */
  def createStream[T: ClassTag](
      jssc: JavaStreamingContext,
      host: String,
      port: Int,
      address: String,
      messageConverter: Message => Option[T],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    new AMQPInputDStream(jssc.ssc, host, port, address, messageConverter, storageLevel)
  }
}
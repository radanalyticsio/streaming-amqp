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

import io.radanalytics.streaming.amqp.{AMQPBodyFunction, AMQPInputDStream, JavaAMQPJsonFunction}
import org.apache.qpid.proton.message.Message
import org.apache.spark.api.java.function.Function
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.util.WriteAheadLogUtils

import scala.reflect.ClassTag

object AMQPUtils {

  /**
    * Create an input stream that receives messages from an AMQP sender node
    *
    * @param ssc              Spark Streaming context
    * @param host             AMQP container hostname or IP address to connect
    * @param port             AMQP container port to connect
    * @param username         Username for SASL PLAIN authentication
    * @param password         Password for SASL PLAIN authentication
    * @param address          AMQP node address on which receive messages
    * @param messageConverter Callback for converting AMQP message to custom type at application level
    * @param storageLevel     RDD storage level
    */
  def createStream[T: ClassTag](
       ssc: StreamingContext,
       host: String,
       port: Int,
       username: Option[String],
       password: Option[String],
       address: String,
       messageConverter: Message => Option[T],
       storageLevel: StorageLevel
     ): ReceiverInputDStream[T] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new AMQPInputDStream(ssc, host, port, username, password, address, messageConverter, walEnabled, storageLevel)
  }

  /**
    * Create an input stream that receives messages from an AMQP sender node
    *
    * @param ssc     Spark Streaming context
    * @param host    AMQP container hostname or IP address to connect
    * @param port    AMQP container port to connect
    * @param username Username for SASL PLAIN authentication
    * @param password Password for SASL PLAIN authentication
    * @param address AMQP node address on which receive messages
    * @note Default message converter try to convert the AMQP message body into the custom type T
    */
  def createStream[T: ClassTag](
       ssc: StreamingContext,
       host: String,
       port: Int,
       username: Option[String],
       password: Option[String],
       address: String
     ): ReceiverInputDStream[T] = {
    createStream(ssc, host, port, username, password, address, new AMQPBodyFunction[T], StorageLevel.MEMORY_ONLY)
  }

  /**
    * Create an input stream that receives messages from an AMQP sender node
    *
    * @param jssc             Java Spark Streaming context
    * @param host             AMQP container hostname or IP address to connect
    * @param port             AMQP container port to connect
    * @param username         Username for SASL PLAIN authentication
    * @param password         Password for SASL PLAIN authentication
    * @param address          AMQP node address on which receive messages
    * @param messageConverter Callback for converting AMQP message to custom type at application level
    * @param storageLevel     RDD storage level
    * @note Default message converter try to convert the AMQP message body into the custom type T                              *
    */
  def createStream[T](
       jssc: JavaStreamingContext,
       host: String,
       port: Int,
       username: Option[String],
       password: Option[String],
       address: String,
       messageConverter: Function[Message, Option[T]],
       storageLevel: StorageLevel
     ): JavaReceiverInputDStream[T] = {

    def fn: (Message) => Option[T] = (x: Message) => messageConverter.call(x)
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]

    val walEnabled = WriteAheadLogUtils.enableReceiverLog(jssc.ssc.conf)

    new AMQPInputDStream(jssc.ssc, host, port, username, password, address, fn, walEnabled, storageLevel)
  }

  /**
    * Create an input stream that receives messages from an AMQP sender node
    *
    * @param jssc    Java Spark Streaming context
    * @param host    AMQP container hostname or IP address to connect
    * @param port    AMQP container port to connect
    * @param username Username for SASL PLAIN authentication
    * @param password Password for SASL PLAIN authentication
    * @param address AMQP node address on which receive messages
    * @note Default message converter try to convert the AMQP message body into the JSON string representation
    */
  def createStream(
       jssc: JavaStreamingContext,
       host: String,
       port: Int,
       username: Option[String],
       password: Option[String],
       address: String
     ): JavaReceiverInputDStream[String] = {

    // define the default message converted
    val messageConverter: Function[Message, Option[String]] = new JavaAMQPJsonFunction()

    createStream(jssc, host, port, username, password, address, messageConverter, StorageLevel.MEMORY_ONLY)
  }
}

/**
  * Helper class that wraps the methods in AMQPUtils into more Python-friendly class and
  * function so that it can be easily instantiated and called from Python's AMQPUtils.
  */
private [amqp]
class AMQPUtilsPythonHelper {

  def createStream(
       jssc: JavaStreamingContext,
       host: String,
       port: Int,
       address: String
     ): JavaDStream[String] = {

    AMQPUtils.createStream(jssc, host, port, None, None, address)
  }
}

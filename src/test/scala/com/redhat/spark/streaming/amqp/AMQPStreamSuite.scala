/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.spark.streaming.amqp

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.apache.spark.SparkConf

/**
 * Scala test suite for the AMQP input stream
 */
class AMQPStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {
  
  private val batchDuration: Duration = Seconds(1)
  private val master: String = "local[2]"
  private val appName: String = this.getClass().getSimpleName()
  private val address: String = "my_address"
  
  private var conf: SparkConf = _
  private var ssc: StreamingContext = _
  private var amqpTestUtils: AMQPTestUtils = _
  
  before {
    
    conf = new SparkConf().setMaster(master).setAppName(appName)
    ssc = new StreamingContext(conf, batchDuration)
    
    amqpTestUtils = new AMQPTestUtils()
    amqpTestUtils.setup()
  }
  
  after {
    
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }
  
  test("amqp receive") {
    
    val sendMessage = "Spark Streaming & AMQP"
    val receiveStream = AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port, address, StorageLevel.MEMORY_ONLY)
    
    var receiveMessage: List[String] = List()
    receiveStream.foreachRDD(rdd => {
      receiveMessage = receiveMessage ::: List(rdd.first())
    })
    
    ssc.start()
    
    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendSimpleMessage(address, sendMessage)
      assert(sendMessage.equals(receiveMessage(0)))
    }
    ssc.stop()
  }
}
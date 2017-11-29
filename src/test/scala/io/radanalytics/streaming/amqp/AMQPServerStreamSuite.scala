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

package io.radanalytics.streaming.amqp

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._

/**
 * Scala test suite for the AMQP input stream
 */
class AMQPServerStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {
  
  private val batchDuration: Duration = Seconds(1)
  private val master: String = "local[2]"
  private val appName: String = this.getClass().getSimpleName()
  private val address: String = "my_address"
  private val checkpointDir: String = "/tmp/spark-streaming-amqp-tests"
  
  private var conf: SparkConf = _
  private var ssc: StreamingContext = _
  private var amqpTestUtils: AMQPTestUtils = _

  before {
    
    conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)
    
    amqpTestUtils = new AMQPTestUtils()
    amqpTestUtils.setup()
  }
  
  after {

    if (ssc != null) {
      ssc.stop()
    }

    if (amqpTestUtils != null) {
      amqpTestUtils.teardown()
    }
  }

  test("AMQP receive server") {

    val sendMessage = "Spark Streaming & AMQP"
    val max = 10
    val delay = 100l

    amqpTestUtils.startAMQPServer(sendMessage, max, delay)

    val converter = new AMQPBodyFunction[String]

    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)

    var receivedMessage: List[String] = List()
    receiveStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: rdd.collect().toList
      }
    })

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {

      assert(receivedMessage.length == max)
    }
    ssc.stop()

    amqpTestUtils.stopAMQPServer()
  }
}
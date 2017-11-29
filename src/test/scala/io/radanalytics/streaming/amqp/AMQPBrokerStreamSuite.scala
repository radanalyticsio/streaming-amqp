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

import java.util.Map.Entry
import java.util.{Base64, Iterator}

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.amqp.AMQPUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

/**
 * Scala test suite for the AMQP input stream
 */
class AMQPBrokerStreamSuite extends SparkFunSuite with Eventually with BeforeAndAfter {
  
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

    amqpTestUtils.startBroker()
  }
  
  after {

    amqpTestUtils.stopBroker()
    
    if (ssc != null) {
      ssc.stop()
    }

    if (amqpTestUtils != null) {
      amqpTestUtils.teardown()
    }
  }

  test("AMQP receive simple body string") {

    val converter = new AMQPBodyFunction[String]

    val sendMessage = "Spark Streaming & AMQP"
    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)
    
    var receivedMessage: List[String] = List()
    receiveStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: List(rdd.first())
      }
    })
    
    ssc.start()
    
    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendSimpleMessage(address, sendMessage)
      assert(sendMessage.equals(receivedMessage(0)))
    }
    ssc.stop()
  }

  test("AMQP receive list body") {

    val converter = new AMQPJsonFunction()

    val list: List[Any] = List("a string", 1, 2)
    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)

    val listStream = receiveStream.map(jsonMsg => {

      val mapper: ObjectMapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      var listFinal: ListBuffer[String] = ListBuffer[String]()

      // get an iterator on "section" that is actually an array
      val iterator: Iterator[JsonNode] = mapper.readTree(jsonMsg).get("body").get("section").asInstanceOf[ArrayNode].elements()
      while(iterator.hasNext) {
        listFinal += iterator.next().asText()
      }

      listFinal.mkString(",")
    })

    var receivedMessage: List[String] = List()
    listStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: List(rdd.first())
      }
    })

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendComplexMessage(address, list)
      assert(list.mkString(",").equals(receivedMessage(0)))
    }
    ssc.stop()
  }

  test("AMQP receive map body") {

    val converter = new AMQPJsonFunction()

    val map:Map[_,_] = Map("field_a" -> "a string", "field_b" -> 1)
    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)

    val mapStream = receiveStream.map(jsonMsg => {

      val mapper: ObjectMapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      var listFinal: ListBuffer[String] = ListBuffer[String]()

      // get an iterator on all fields of "section" that is actually a map
      val iterator: Iterator[Entry[String, JsonNode]] = mapper.readTree(jsonMsg).get("body").get("section").fields()
      while(iterator.hasNext) {
        val entry: Entry[String, JsonNode] = iterator.next()
        listFinal += entry.getKey + "=" + entry.getValue.asText()
      }

      listFinal.mkString(",")
    })

    var receivedMessage: List[String] = List()
    mapStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: List(rdd.first())
      }
    })

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendComplexMessage(address, map)
      assert(map.map(t => t._1 + "=" + t._2).mkString(",").equals(receivedMessage(0)))
    }
    ssc.stop()
  }

  test("AMQP receive array body") {

    val converter = new AMQPJsonFunction()

    val array: Array[Any] = Array(1, 2)
    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)

    val listStream = receiveStream.map(jsonMsg => {

      val mapper: ObjectMapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      var listFinal: ListBuffer[String] = ListBuffer[String]()

      // get an iterator on "section" that is actually an array
      val iterator: Iterator[JsonNode] = mapper.readTree(jsonMsg).get("body").get("section").asInstanceOf[ArrayNode].elements()
      while(iterator.hasNext) {
        listFinal += iterator.next().asText()
      }

      listFinal.mkString(",")
    })

    var receivedMessage: List[String] = List()
    listStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: List(rdd.first())
      }
    })

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendComplexMessage(address, array)
      assert(array.mkString(",").equals(receivedMessage(0)))
    }
    ssc.stop()
  }

  test("AMQP receive binary body") {

    val converter = new AMQPJsonFunction()

    val sendMessage = "Spark Streaming & AMQP"
    val receiveStream =
      AMQPUtils.createStream(ssc, amqpTestUtils.host, amqpTestUtils.port,
        amqpTestUtils.username, amqpTestUtils.password, address, converter, StorageLevel.MEMORY_ONLY)

    val binaryStream = receiveStream.map(jsonMsg => {

      val mapper: ObjectMapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      val body: String = new String(Base64.getDecoder.decode(mapper.readTree(jsonMsg).get("body").get("section").asText()))

      body
    })

    var receivedMessage: List[String] = List()
    binaryStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        receivedMessage = receivedMessage ::: List(rdd.first())
      }
    })

    ssc.start()

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      amqpTestUtils.sendBinaryMessage(address, sendMessage.getBytes)
      assert(sendMessage.equals(receivedMessage(0)))
    }
    ssc.stop()
  }

}
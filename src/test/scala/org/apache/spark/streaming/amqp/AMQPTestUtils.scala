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

import java.net.URI

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.proton._
import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.apache.qpid.proton.message.Message

/**
 * Scala test utilities for the AMQP input stream
 */
class AMQPTestUtils {

  private var broker: BrokerService = _

  val host: String = "localhost"
  val port: Int = 5672
  val address: String = "my_address"

  var vertx: Vertx = _
  
  def setup(): Unit = {

    broker = new BrokerService()
    broker.setPersistent(false)

    val brokerUri: String = s"$host:$port"
    // add AMQP connector
    val amqpConnector: TransportConnector = new TransportConnector()
    amqpConnector.setName("AMQP")
    amqpConnector.setUri(new URI(s"amqp://$brokerUri"))
    broker.addConnector(amqpConnector)

    broker.start()
    broker.waitUntilStarted()

    vertx = Vertx.vertx()
  }
  
  def teardown(): Unit = {

    if (broker != null) {
      broker.stop()
      broker.waitUntilStopped()
    }

    if (vertx != null) {
      vertx.close()
    }
  }
  
  /**
   * Send a simple message
   * @param	address			AMQP address node to which sending the message
   * @param	body				AMQP body for the message to send
   */
  def sendSimpleMessage(address: String, body: String): Unit = {
    
    val client: ProtonClient = ProtonClient.create(vertx)
    
    client.connect(host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
        if (ar.succeeded()) {
          
          val connection: ProtonConnection = ar.result()
          connection.open()
          
          val sender: ProtonSender = connection.createSender(null)
          sender.open()
          
          val message: Message = ProtonHelper.message(address, body)
          sender.send(message, new Handler[ProtonDelivery] {
            override def handle(delivery: ProtonDelivery): Unit = {

              sender.close()
              connection.close()
            }
          })
        }
      }
    })
    
  }
}
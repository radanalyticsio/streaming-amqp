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

import java.lang.Long
import java.net.URI
import java.util.{ArrayList, HashMap}

import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.proton._
import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Data}
import org.apache.qpid.proton.message.Message

import scala.collection.JavaConverters._

/**
 * Scala test utilities for the AMQP input stream
 */
class AMQPTestUtils {

  private var broker: BrokerService = _

  private var server: ProtonServer = _

  val host: String = "localhost"
  val port: Int = 5672
  val address: String = "my_address"
  val username: Option[String] = None
  val password: Option[String] = None

  var vertx: Vertx = _

  def setup(): Unit = {

    vertx = Vertx.vertx()
  }

  def teardown(): Unit = {

    if (vertx != null) {
      vertx.close()
    }
  }

  /**
   * Start and embedded ActiveMQ broker
   */
  def startBroker(): Unit = {

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
  }

  /**
   * Stop the embedded ActiveMQ broker
   */
  def stopBroker(): Unit = {

    if (broker != null) {
      broker.stop()
      broker.waitUntilStopped()
    }
  }

  /**
   * Send a simple message
   *
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

  /**
    * Send a message with a complex body (array, list, map)
    *
    * @param address    AMQP address node to which sending the message
    * @param body       AMQP body for the message to send (i.e. array, list, map)
    */
  def sendComplexMessage(address: String, body: Any): Unit = {

    val client: ProtonClient = ProtonClient.create(vertx)

    client.connect(host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
        if (ar.succeeded()) {

          val connection: ProtonConnection = ar.result()
          connection.open()

          val sender: ProtonSender = connection.createSender(null)
          sender.open()

          val message: Message = Proton.message()
          message.setAddress(address);

          body match {
            case list: List[_] => message.setBody(new AmqpValue(list.asJava))
            case array: Array[_] => message.setBody(new AmqpValue(array))
            case map: Map[_,_] => message.setBody(new AmqpValue(map.asJava))
            case arrayList: ArrayList[_] => message.setBody(new AmqpValue(arrayList))
            case hashMap: HashMap[_,_] => message.setBody(new AmqpValue(hashMap))
          }

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

  /**
    * Send a message with a binary body
    *
    * @param address    AMQP address node to which sending the message
    * @param body       AMQP binary body for the message to send
    */
  def sendBinaryMessage(address: String, body: Array[Byte]): Unit = {

    val client: ProtonClient = ProtonClient.create(vertx)

    client.connect(host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
        if (ar.succeeded()) {

          val connection: ProtonConnection = ar.result()
          connection.open()

          val sender: ProtonSender = connection.createSender(null)
          sender.open()

          val message: Message = Proton.message()
          message.setAddress(address);
          message.setBody(new Data(new Binary(body)))

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

  /**
   * Start an AMQP server which sends messages on an attached link by a receiver
   * @param body    Body for the message to send
   * @param max     Number of messages to send
   * @param delay   Delay between two consecutive messages to send
   */
  def startAMQPServer(body: String, max: Int, delay: Long): Unit = {

    val options: ProtonServerOptions = new ProtonServerOptions();
    options.setHost(host)
    options.setPort(port)

    server = ProtonServer.create(vertx, options)
      .connectHandler(new Handler[ProtonConnection] {
        override def handle(connection: ProtonConnection): Unit = {

          connection
            .open()
            .sessionOpenHandler(new Handler[ProtonSession] {
              override def handle(session: ProtonSession): Unit = {
                session.open()
              }
            })


          connection.senderOpenHandler(new Handler[ProtonSender] {
            override def handle(sender: ProtonSender): Unit = {

              sender.open()
              var count: Int = 0
              vertx.setPeriodic(delay, new Handler[Long] {
                override def handle(timer: Long): Unit = {

                  if (count < max) {
                    count += 1
                    val message: Message = ProtonHelper.message(body)
                    sender.send(message)
                  } else {
                    vertx.cancelTimer(timer)
                  }

                }
              })

            }
          })

        }
      })
      .listen(new Handler[AsyncResult[ProtonServer]] {
        override def handle(ar: AsyncResult[ProtonServer]): Unit = {

          if (ar.succeeded()) {

          } else {

          }
        }
      })
  }

  /**
   * Stop the AMQP server
   */
  def stopAMQPServer(): Unit = {

    if (server != null) {
      server.close()
    }
  }

}
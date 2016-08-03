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

import io.vertx.core.{AsyncResult, Context, Handler, Vertx}
import io.vertx.proton.{ProtonClient, ProtonClientOptions, ProtonConnection, ProtonDelivery}
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Receiver for getting messages from an AMQP sender node
  *
  * @param host             AMQP container hostname or IP address to connect
  * @param port             AMQP container port to connect
  * @param address          AMQP node address on which receive messages
  * @param messageConverter Callback for converting AMQP message to custom type at application level
  * @param storageLevel	    RDD storage level
  */
private [streaming]
class AMQPReceiver[T](
       host: String,
       port: Int,
       address: String,
       messageConverter: Message => Option[T],
       storageLevel: StorageLevel
     ) extends Receiver[T](storageLevel) with Logging with AMQPFlowControllerListener {

  protected var flowController: AMQPFlowController = _

  protected var context: Context = _
  protected var vertx: Vertx = _

  protected var client: ProtonClient = _

  protected var connection: ProtonConnection = _

  override def onStart(): Unit = {

    logInfo("onStart")

    vertx = Vertx.vertx()

    // just used if some future options will be useful
    val options: ProtonClientOptions = new ProtonClientOptions()

    client = ProtonClient.create(vertx)

    client.connect(options, host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {

        if (ar.succeeded()) {

          // get the Vert.x context created internally by the Proton library
          context = vertx.getOrCreateContext()

          connection = ar.result()
          processConnection(connection)

        } else {

          restart("Connection to AMQP address not established", ar.cause())
        }

      }
    })
  }

  override def onStop(): Unit = {

    logInfo("onStop")

    if (Option(connection).isDefined) {
      connection.close()
    }

    if (Option(flowController).isDefined) {
      flowController.close()
    }

    if (Option(vertx).isDefined) {
      vertx.close()
    }
  }

  /**
    * Process the connection established with the AMQP source
    *
    * @param connection     AMQP connection instance
    */
  private def processConnection(connection: ProtonConnection): Unit = {

    connection
      .closeHandler(new Handler[AsyncResult[ProtonConnection]] {
        override def handle(ar: AsyncResult[ProtonConnection]): Unit = {

          // handling connection closed at AMQP level ("close" performative)
          if (ar.succeeded()) {
            restart(s"Connection closed by peer ${ar.result().getRemoteContainer}")
          } else {
            restart("Connection closed by peer", ar.cause())
          }

        }
      })
      .disconnectHandler(new Handler[ProtonConnection] {
        override def handle(connection: ProtonConnection): Unit = {

          // handling connection closed at TCP level (disconnection)
          restart(s"Disconnection by peer ${connection.getRemoteContainer}")
        }
      })
      .open()

    val receiver = connection.createReceiver(address)

    // after created, the AMQP receiver lifecycle is tied to the flow controller
    // current receiver instance is needed as a listener for flow controller events
    flowController = new AMQPFlowController(receiver, this)
    flowController.open()
  }

  /**
    * Called when an AMQP message is received on the link
    *
    * @param delivery Proton delivery instance
    * @param message  Proton AMQP message
    */
  override def onAcquire(delivery: ProtonDelivery, message: Message): Unit = {

    store(messageConverter(message).get)
  }
}

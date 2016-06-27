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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.proton.ProtonDelivery
import io.vertx.proton.ProtonClient
import io.vertx.proton.ProtonClientOptions
import io.vertx.proton.ProtonConnection
import io.vertx.proton.ProtonMessageHandler
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.amqp.messaging.Section
import org.apache.qpid.proton.amqp.messaging.AmqpValue

/**
 * Receiver for getting messages from an AMQP sender node
 * @param host					    AMQP container hostname or IP address to connect
 * @param port					    AMQP container port to connect
 * @param address				    AMQP node address on which receive messages
 * @param messageConverter  Callback for converting AMQP message to custom type at application level
 * @param storageLevel	    RDD storage level
 */
class AMQPReceiver[T](
      host: String,
      port: Int,
      address: String,
      messageConverter: Message => Option[T],
      storageLevel: StorageLevel
    ) extends Receiver[T](storageLevel) {
  
  var vertx: Vertx = _
  
  var client: ProtonClient = _
  
  var connection: ProtonConnection = _
  
  def onStart() {

    vertx = Vertx.vertx()
    
    val options: ProtonClientOptions = new ProtonClientOptions()
    
    client = ProtonClient.create(vertx)
    
    client.connect(options, host, port, new Handler[AsyncResult[ProtonConnection]] {
      override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
        
        if (ar.succeeded()) {
      
          connection = ar.result()
          processConnection(connection)
          
        } else {
          
        }
        
      }
    })

  }
  
  def onStop() {
    
    connection.close()
    vertx.close()
  }
  
  private def processConnection(connection: ProtonConnection): Unit = {
    
    connection
      .closeHandler(new Handler[AsyncResult[ProtonConnection]] {
        override def handle(ar: AsyncResult[ProtonConnection]): Unit = {
          restart("Connection closed")
        }
      })
      .open()
    
    connection
      .createReceiver(address)
      .handler(new ProtonMessageHandler() {
        override def handle(delivery: ProtonDelivery, message: Message): Unit = {
          store(messageConverter(message).get)
        }
      })
      .open()
    
  }
  
  implicit def functionToHandler[A](f: A => Unit): Handler[A] = new Handler[A] {
    override def handle(event: A): Unit = {
      f(event)
    }
  }
}
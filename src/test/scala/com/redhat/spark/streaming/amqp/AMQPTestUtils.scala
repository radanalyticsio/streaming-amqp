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

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.proton.ProtonClient
import io.vertx.proton.ProtonConnection
import io.vertx.proton.ProtonDelivery
import io.vertx.proton.ProtonSender
import org.apache.qpid.proton.message.Message
import io.vertx.proton.ProtonHelper

/**
 * Scala test utilities for the AMQP input stream
 */
class AMQPTestUtils {
  
  val host: String = "localhost"
  val port: Int = 5672
  val address: String = "my_address"
  
  var vertx: Vertx = _
  
  def setup(): Unit = {
    
    vertx = Vertx.vertx()
  }
  
  def teardown(): Unit = {
    
    vertx.close()
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
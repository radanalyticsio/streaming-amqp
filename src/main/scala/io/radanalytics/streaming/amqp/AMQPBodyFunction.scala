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

import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Section}
import org.apache.qpid.proton.message.Message

/**
  * Provides implementation for a function which has an AMQP messages as input
  * and provide a different type T instance as output
  *
  * @tparam T
  */
class AMQPBodyFunction[T] extends ((Message) => Option[T]) with Serializable {

  override def apply(message: Message): Option[T] = {

    val body: Section = message.getBody()
    if (body.isInstanceOf[AmqpValue]) {
      val content: T = body.asInstanceOf[AmqpValue].getValue().asInstanceOf[T]
      Some(content)
    } else {
      None
    }
  }
}

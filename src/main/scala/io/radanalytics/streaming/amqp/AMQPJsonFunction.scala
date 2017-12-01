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

import java.util.{Base64, List, Map}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.qpid.proton.amqp.Symbol
import org.apache.qpid.proton.amqp.messaging._
import org.apache.qpid.proton.message.Message

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Provides implementation for a function which has an AMQP messages as input
  * and provide a JSON representation as output
  */
class AMQPJsonFunction extends Function1[Message, Option[String]] with Serializable {

  // AMQP message section to encode in JSON
  private final val APPLICATION_PROPERTIES: String = "applicationProperties"
  private final val PROPERTIES: String = "properties"
  private final val MESSAGE_ANNOTATIONS: String = "messageAnnotations"
  private final val BODY: String = "body"

  private final val SECTION_TYPE: String = "type"
  private final val SECTION: String = "section"
  private final val SECTION_AMQP_VALUE_TYPE: String = "amqpValue"
  private final val SECTION_DATA_TYPE: String = "data"

  // main AMQP properties
  private final val MESSAGE_ID: String = "messageId"
  private final val TO: String = "to"
  private final val SUBJECT: String = "subject"
  private final val REPLY_TO: String = "replyTo"
  private final val CORRELATION_ID: String = "correlationId"

  override def apply(message: Message): Option[String] = {

    val mapper: ObjectMapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // root JSON
    val json: ObjectNode = mapper.createObjectNode()

    // make JSON with AMQP properties
    val jsonProperties: ObjectNode = json.putObject(PROPERTIES)

    if (Option(message.getMessageId).isDefined)
      jsonProperties.put(MESSAGE_ID, message.getMessageId.toString)
    if (Option(message.getAddress).isDefined)
      jsonProperties.put(TO, message.getAddress)
    if (Option(message.getSubject).isDefined)
      jsonProperties.put(SUBJECT, message.getSubject)
    if (Option(message.getReplyTo).isDefined)
      jsonProperties.put(REPLY_TO, message.getReplyTo)
    if (Option(message.getCorrelationId).isDefined)
      jsonProperties.put(CORRELATION_ID, message.getCorrelationId.toString)

    // make JSON with AMQP application properties
    val applicationProperties: Option[ApplicationProperties] = Option(message.getApplicationProperties)

    if (applicationProperties.isDefined) {

      val jsonApplicationProperties: ObjectNode = json.putObject(APPLICATION_PROPERTIES)
      val applicationPropertiesMap: mutable.Map[String, AnyRef] = applicationProperties.get.getValue.asInstanceOf[Map[String, AnyRef]].asScala
      for ((k,v) <- applicationPropertiesMap) {
        jsonApplicationProperties.put(k, v.toString)
      }
    }

    // make JSON with AMQP message annotations
    val messageAnnotations: Option[MessageAnnotations] = Option(message.getMessageAnnotations)

    if (messageAnnotations.isDefined) {

      val jsonMessageAnnotations: ObjectNode = json.putObject(MESSAGE_ANNOTATIONS)
      val messageAnnotationsMap: mutable.Map[Symbol, AnyRef] = messageAnnotations.get.getValue.asScala
      for ((k,v) <- messageAnnotationsMap) {
        jsonMessageAnnotations.put(k.toString, v.toString)
      }
    }

    // get body from AMQP message
    val body: Option[Section] = Option(message.getBody)

    if (body.isDefined) {

      val jsonBody: ObjectNode = json.putObject(BODY)

      body.get match {
        // section is AMQP value
        case amqpValue: AmqpValue => {

          jsonBody.put(SECTION_TYPE, SECTION_AMQP_VALUE_TYPE)

          amqpValue.getValue match {

            // encoded as String
            case content: String => {
              jsonBody.put(SECTION, content)
            }
            // encoded as List
            case list: List[_] => {
              val jsonList: ArrayNode = mapper.valueToTree(list)
              jsonBody.putArray(SECTION).addAll(jsonList)
            }
            // encoded as an Array
            case array: Array[_] => {
              val jsonArray: ArrayNode = mapper.valueToTree(array)
              jsonBody.putArray(SECTION).addAll(jsonArray)
            }
            // encoded as a Map
            case map: Map[_,_] => {
              val jsonMap: ObjectNode = mapper.valueToTree(map)
              jsonBody.putObject(SECTION).setAll(jsonMap)
            }
          }
        }
        // section is Data (binary)
        case data: Data => {

          jsonBody.put(SECTION_TYPE, SECTION_DATA_TYPE)

          val value: Array[Byte] = data.getValue.getArray

          // put the section bytes as Base64 encoded string
          jsonBody.put(SECTION, new String(Base64.getEncoder.encode(value)))

        }
        case _ =>
      }

    }

    Some(json.toString)
  }
}

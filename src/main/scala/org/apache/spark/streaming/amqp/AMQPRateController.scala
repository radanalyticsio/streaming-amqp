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

import java.util.concurrent.TimeUnit._
import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}
import io.vertx.proton.ProtonDelivery
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging

/**
  * Provides message rate control with related throttling
  *
  * @param maxRateLimit       Max rate for receiving messages
  */
abstract class AMQPRateController(
      maxRateLimit: Long
      ) extends Logging {

  // throttling healthy checked for 1 sec + 50%
  private final val throttlingHealthyPeriod = 1500l

  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  private val mutex: AnyRef = new Object()

  private var throttling: Boolean = false
  // timer used in order to raise the throttling end even when no other messages
  // arrive after the first one which caused the throttling start
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private val throttlingHealthy: ThrottlingHealthy = new ThrottlingHealthy()

  private var initialized: Boolean = false

  /**
    * Initialization method
    */
  final def init(): Unit = {

    initialized = true
    doInit()
  }

  /**
    * Open/start the rate controller activity
    */
  final def open(): Unit = {

    if (!initialized) {
      throw new Exception("AMQP rate controller needs to be initialize first")
    }
    doOpen()
  }

  /**
    * Close/end the rate controller activity
    */
  final def close(): Unit = {

    scheduledExecutorService.shutdownNow()
    doClose()
  }

  /**
    * Try to acquire the permit to handle incoming message with related delivery
    *
    * @param delivery       Delivery information
    * @param message        AMQP message received
    */
  final def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    mutex.synchronized {

      // try to acquire the rate limiter in order to have permits at current rate
      if (rateLimiter.tryAcquire()) {

        if (throttling) {
          logInfo("Throttling ended ... ")
          throttling = false
          doThrottlingEnd()

          // throttling ended thanks to acquired permits at current rate
          // no more healthy control is needed
          scheduledExecutorService.shutdownNow()
        }

        doAcquire(delivery, message)

      // permit not acquired, max rate exceeded
      } else {

        if (!throttling) {
          // throttling start now
          throttling = true
          doThrottlingStart()
          logWarning("Throttling started ... ")

          // starting throttling healthy thread in order to end throttling
          // when no more messages are received (silence from sender)
          scheduledExecutorService.schedule(throttlingHealthy, throttlingHealthyPeriod, MILLISECONDS)
        }

        if (throttling) {

          logError("Throttling ... ")
          // already in throttling
          doThrottling(delivery, message)
        }

      }
    }

  }

  def doInit(): Unit = { }

  def doOpen(): Unit = { }

  def doClose(): Unit = { }

  def doAcquire(delivery: ProtonDelivery, message: Message): Unit = { }

  def doThrottlingStart(): Unit = { }

  def doThrottlingEnd(): Unit = { }

  def doThrottling(delivery: ProtonDelivery, message: Message): Unit = { }

  /**
    * Return current max rate
    * @return     Max rate
    */
  final def getCurrentLimit: Long = {

    rateLimiter.getRate.toLong
  }

  /**
    * Runnable class for the throttling healthy checker
    */
  class ThrottlingHealthy extends Runnable {

    override def run(): Unit = {

      mutex.synchronized {

        if (throttling) {

          logInfo("Healthy: Throttling ended ... ")
          throttling = false
          doThrottlingEnd()
        }
      }
    }
  }
}

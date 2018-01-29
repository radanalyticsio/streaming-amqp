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
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture}

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}
import io.vertx.proton.{ProtonDelivery, ProtonMessageHandler, ProtonReceiver}
import org.apache.qpid.proton.amqp.{Symbol => AmqpSymbol}
import org.apache.qpid.proton.amqp.messaging.Rejected
import org.apache.qpid.proton.amqp.transport.ErrorCondition
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.receiver.BlockGenerator

/**
  * Provides message rate control with related throttling
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
abstract class AMQPRateController(
       blockGenerator: BlockGenerator,
       receiver: ProtonReceiver
      ) extends Logging {

  // check on the receiver and block generator instances
  if (Option(receiver).isEmpty)
    throw new IllegalArgumentException("The receiver instance cannot be null")

  if (Option(blockGenerator).isEmpty)
    throw new IllegalArgumentException("The block generator instance cannot be null")

  protected final val AmqpRecvError = "org.apache:amqp-recv-error"
  protected final val AmqpRecvThrottling = "Throttling : Max rate limit exceeded"

  // throttling healthy checked for 1 sec + 50%
  private final val throttlingHealthyPeriod = 1500l

  private lazy val rateLimiter = GuavaRateLimiter.create(blockGenerator.getCurrentLimit.toDouble)

  private val mutex: AnyRef = new Object()

  private var throttling: Boolean = false
  // timer used in order to raise the throttling end even when no other messages
  // arrive after the first one which caused the throttling start
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var scheduledThrottlingHealthy: ScheduledFuture[_] = _
  private val throttlingHealthy: ThrottlingHealthy = new ThrottlingHealthy()

  /**
    * Open/start the rate controller activity
    */
  final def open(): Unit = {

    receiver
    .setAutoAccept(false)
    .handler(new ProtonMessageHandler() {
      override def handle(delivery: ProtonDelivery, message: Message): Unit = {

        // handling received message and related delivery
        acquire(delivery, message)
      }
    })

    // extension point before opening receiver
    beforeOpen()

    receiver.open()
  }

  /**
    * Close/end the rate controller activity
    */
  final def close(): Unit = {

    // extension point before closing receiver
    beforeClose()

    if (Option(receiver).isDefined) {
      receiver.close()
    }

    scheduledExecutorService.shutdown()
    scheduledExecutorService.awaitTermination(1, SECONDS)
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
          onThrottlingEnded()

          // throttling ended thanks to acquired permits at current rate
          // no more healthy control is needed
          scheduledThrottlingHealthy.cancel(false)
        }

        onAcquired(delivery, message)

      // permit not acquired, max rate exceeded
      } else {

        if (!throttling) {
          // throttling start now
          throttling = true
          onThrottlingStarted()
          logWarning("Throttling started ... ")

          // starting throttling healthy thread in order to end throttling
          // when no more messages are received (silence from sender)
          scheduledThrottlingHealthy = scheduledExecutorService.schedule(throttlingHealthy, throttlingHealthyPeriod, MILLISECONDS)
        }

        if (throttling) {

          logError("Throttling ... ")
          // already in throttling
          onThrottling(delivery, message)
        }

      }
    }

  }

  def beforeOpen(): Unit = { }

  def beforeClose(): Unit = { }

  def onAcquired(delivery: ProtonDelivery, message: Message): Unit = { }

  def onThrottlingStarted(): Unit = { }

  def onThrottlingEnded(): Unit = { }

  def onThrottling(delivery: ProtonDelivery, message: Message): Unit = { }

  /**
    * Return current max rate
 *
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
          onThrottlingEnded()
        }
      }
    }
  }
}

/**
  * AMQP rate controller implementation using "prefetch"
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
private final class AMQPPrefetchRateController(
                                                blockGenerator: BlockGenerator,
                                                receiver: ProtonReceiver
                                              ) extends AMQPRateController(blockGenerator, receiver) {

  override def beforeOpen(): Unit = {

    // if MaxValue or negative it means no max rate limit specified in the Spark configuration
    // so the prefetch isn't explicitly set but default Vert.x Proton value is used
    if ((blockGenerator.getCurrentLimit != Long.MaxValue) && (blockGenerator.getCurrentLimit >= 0))
      receiver.setPrefetch(blockGenerator.getCurrentLimit.toInt)

    super.beforeOpen()
  }

  override def beforeClose(): Unit = {

    super.beforeClose()
  }

  override def onAcquired(delivery: ProtonDelivery, message: Message): Unit = {

    // only AMQP message will be stored into BlockGenerator internal buffer;
    // delivery is passed as metadata to onAddData and saved here internally
    blockGenerator.addDataWithCallback(message, delivery)

    super.onAcquired(delivery, message)
  }

  override def onThrottlingStarted(): Unit = {

    super.onThrottlingStarted()
  }

  override def onThrottlingEnded(): Unit = {

    super.onThrottlingEnded()
  }

  override def onThrottling(delivery: ProtonDelivery, message: Message): Unit = {

    // during throttling (max rate limit exceeded), all messages are rejected
    val rejected: Rejected = new Rejected()
    val errorCondition: ErrorCondition = new ErrorCondition(AmqpSymbol.valueOf(AmqpRecvError), AmqpRecvThrottling)
    rejected.setError(errorCondition)
    delivery.disposition(rejected, true)

    super.onThrottling(delivery, message)
  }
}

/**
  * AMQP rate controller implementation using "manual" flow control
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
private final class AMQPManualRateController(
                                              blockGenerator: BlockGenerator,
                                              receiver: ProtonReceiver
                                            ) extends AMQPRateController(blockGenerator, receiver) {

  private final val CreditsDefault = 1000
  private final val CreditsThreshold = 0

  var count = 0
  var credits = 0

  override def beforeOpen(): Unit = {

    count = 0

    // if MaxValue or negative it means no max rate limit specified in the Spark configuration
    if ((blockGenerator.getCurrentLimit != Long.MaxValue) && (blockGenerator.getCurrentLimit >= 0)) {
      credits = blockGenerator.getCurrentLimit.toInt
    } else {
      credits = CreditsDefault
    }

    // disable prefetch in order to use manual flow control
    receiver.setPrefetch(0)
    // grant the first bunch of credits
    receiver.flow(credits)

    super.beforeOpen()
  }

  override def beforeClose(): Unit = {

    super.beforeClose()
  }

  override def onAcquired(delivery: ProtonDelivery, message: Message): Unit = {

    // only AMQP message will be stored into BlockGenerator internal buffer;
    // delivery is passed as metadata to onAddData and saved here internally
    blockGenerator.addDataWithCallback(message, delivery)

    count += 1
    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {
      receiver.flow(credits - CreditsThreshold)
      count = 0
    }

    super.onAcquired(delivery, message)
  }

  override def onThrottlingStarted(): Unit = {

    super.onThrottlingStarted()
  }

  override def onThrottlingEnded(): Unit = {

    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {
      receiver.flow(credits - CreditsThreshold)
      count = 0
    }

    super.onThrottlingEnded()
  }

  override def onThrottling(delivery: ProtonDelivery, message: Message): Unit = {

    count += 1

    // during throttling (max rate limit exceeded), all messages are rejected
    val rejected: Rejected = new Rejected()
    val errorCondition: ErrorCondition = new ErrorCondition(AmqpSymbol.valueOf(AmqpRecvError), AmqpRecvThrottling)
    rejected.setError(errorCondition)
    delivery.disposition(rejected, true)

    super.onThrottling(delivery, message)
  }
}

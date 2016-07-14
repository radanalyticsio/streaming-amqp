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
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.proton._
import org.apache.qpid.proton.amqp.messaging.Rejected
import org.apache.qpid.proton.amqp.transport.{ErrorCondition, Source, Target}
import org.apache.qpid.proton.amqp.{Symbol => AmqpSymbol}
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging

/**
  * Abstract class for throttling enabled receiver
  * (decorator for a ProtonReceiver)
  *
  * @param maxRateLimit     Max rate limit for throttling
  * @param receiver         AMQP receiver instance
  */
abstract class ThrottleProtonReceiver(
                 maxRateLimit: Long,
                 receiver: ProtonReceiver
                 ) extends ProtonReceiver with Logging {

  // check on the receiver instance
  if (receiver == null)
    throw new IllegalArgumentException("The receiver instance cannot be null")

  protected final val AmqpRecvError = "org.apache:amqp-recv-error"
  protected final val AmqpRecvThrottling = "Throttling : Max rate limit exceeded"

  // throttling healthy checked for 1 sec + 50%
  private final val throttlingHealthyPeriod = 1500l

  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  private val mutex: AnyRef = new Object()

  private var throttling: Boolean = false
  // timer used in order to raise the throttling end even when no other messages
  // arrive after the first one which caused the throttling start
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var scheduledThrottlingHealthy: ScheduledFuture[_] = _
  private val throttlingHealthy: ThrottlingHealthy = new ThrottlingHealthy()

  // application defined message handler for handling a received message
  private var messageHandler: ProtonMessageHandler = _

  /**
    * Try to acquire the permit to handle incoming message with related delivery
    *
    * @param delivery       Delivery information
    * @param message        AMQP message received
    */
  private def acquire(delivery: ProtonDelivery, message: Message): Unit = {

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

        // call the application provided handler
        messageHandler.handle(delivery, message)

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

  // *** Extension points for derived classes ***

  protected def beforeOpen(): Unit = { }

  protected def beforeClose(): Unit = { }

  protected def onAcquired(delivery: ProtonDelivery, message: Message): Unit = { }

  protected def onThrottlingStarted(): Unit = { }

  protected def onThrottlingEnded(): Unit = { }

  protected def onThrottling(delivery: ProtonDelivery, message: Message): Unit = { }

  // *** ProtonReceiver interface implementation ***

  override def open(): ProtonReceiver = {

    // extension point before opening receiver
    beforeOpen()

    receiver.open()

    return this
  }

  override def close(): ProtonReceiver = {

    // extension point before closing receiver
    beforeClose()

    if (receiver != null) {
      receiver.close()
    }

    scheduledExecutorService.shutdown()
    scheduledExecutorService.awaitTermination(1, SECONDS)

    return this
  }

  override def handler(handler: ProtonMessageHandler): ProtonReceiver = {

    messageHandler = handler

    // the application defined message handler is called inside the acquire method
    // which provides the throttling logic around it
    receiver.handler(new ProtonMessageHandler {
      override def handle(delivery: ProtonDelivery, message: Message): Unit = {

        // handling received message and related delivery
        acquire(delivery, message)
      }
    })

    return this
  }

  override def getPrefetch: Int = { receiver.getPrefetch() }

  override def setPrefetch(messages: Int): ProtonReceiver = {
    throw new Exception("Not allowed for a throttle receiver")
  }

  override def flow(credits: Int): ProtonReceiver = {
    throw new Exception("Not allowed for a throttle receiver")
  }

  override def isAutoAccept: Boolean = { receiver.isAutoAccept() }

  override def setAutoAccept(autoAccept: Boolean): ProtonReceiver = {
    receiver.setAutoAccept(autoAccept)
    this
  }

  override def closeHandler(remoteCloseHandler: Handler[AsyncResult[ProtonReceiver]]): ProtonReceiver = {
    receiver.closeHandler(remoteCloseHandler)
    this
  }

  override def openHandler(remoteOpenHandler: Handler[AsyncResult[ProtonReceiver]]): ProtonReceiver = {
    receiver.openHandler(remoteOpenHandler)
    this
  }

  override def setCondition(condition: ErrorCondition): ProtonReceiver = {
    receiver.setCondition(condition)
    this
  }

  override def getRemoteCondition: ErrorCondition = { receiver.getRemoteCondition() }

  override def getRemoteTarget: Target = { receiver.getRemoteTarget() }

  override def setTarget(target: Target): ProtonReceiver = {
    receiver.setTarget(target)
    this
  }

  override def setSource(source: Source): ProtonReceiver = {
    receiver.setSource(source)
    this
  }

  override def getRemoteSource: Source = { receiver.getRemoteSource() }

  override def isOpen: Boolean = { receiver.isOpen() }

  override def getTarget: Target = { receiver.getTarget() }

  override def getCondition: ErrorCondition = { receiver.getCondition() }

  override def getSource: Source = { receiver.getSource() }

  override def getQoS: ProtonQoS = { receiver.getQoS() }

  override def getSession: ProtonSession = { receiver.getSession() }

  override def getRemoteQoS: ProtonQoS = { receiver.getRemoteQoS() }

  override def setQoS(qos: ProtonQoS): ProtonReceiver = {
    receiver.setQoS(qos)
    this
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
  * Throttle receiver implementation using the "prefetch" flow control feature
 *
  * @param maxRateLimit     Max rate limit for throttling
  * @param receiver         AMQP receiver instance
  */
class ThrottlePrefetchReceiver(
         maxRateLimit: Long,
         receiver: ProtonReceiver
         ) extends ThrottleProtonReceiver(maxRateLimit, receiver) {

  override protected def beforeOpen(): Unit = {

    // if MaxValue or negative it means no max rate limit specified in the Spark configuration
    // so the prefetch isn't explicitly set but default Vert.x Proton value is used
    if ((maxRateLimit != Long.MaxValue) && (maxRateLimit >= 0))
      receiver.setPrefetch(maxRateLimit.toInt)

    super.beforeOpen()
  }

  override protected def beforeClose(): Unit = {

    super.beforeClose()
  }

  override def onAcquired(delivery: ProtonDelivery, message: Message): Unit = {

    super.onAcquired(delivery, message)
  }

  override protected def onThrottlingStarted(): Unit = {

    super.onThrottlingStarted()
  }

  override protected def onThrottlingEnded(): Unit = {

    super.onThrottlingEnded()
  }

  override protected def onThrottling(delivery: ProtonDelivery, message: Message): Unit = {

    // during throttling (max rate limit exceeded), all messages are rejected
    val rejected: Rejected = new Rejected()
    val errorCondition: ErrorCondition = new ErrorCondition(AmqpSymbol.valueOf(AmqpRecvError), AmqpRecvThrottling)
    rejected.setError(errorCondition)
    delivery.disposition(rejected, true)

    super.onThrottling(delivery, message)
  }
}

/**
  * Throttle receiver implementation using the "manual" flow control
 *
  * @param maxRateLimit     Max rate limit for throttling
  * @param receiver         AMQP receiver instance
  */
class ThrottleManualReceiver(
         maxRateLimit: Long,
         receiver: ProtonReceiver
       ) extends ThrottleProtonReceiver(maxRateLimit, receiver) {

  private final val CreditsDefault = 1000
  private final val CreditsThreshold = 0

  var count = 0
  var credits = 0

  override def beforeOpen(): Unit = {

    count = 0

    // if MaxValue or negative it means no max rate limit specified in the Spark configuration
    if ((maxRateLimit != Long.MaxValue) && (maxRateLimit >= 0)) {
      credits = maxRateLimit.toInt
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

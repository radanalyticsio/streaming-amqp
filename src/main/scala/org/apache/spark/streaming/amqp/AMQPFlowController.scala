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

import java.lang.Long
import java.util.concurrent._

import io.vertx.core.{Handler, Vertx}
import io.vertx.proton.{ProtonDelivery, ProtonMessageHandler, ProtonReceiver}
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Listener for events from an AMQP flow controller
  */
private [streaming]
trait AMQPFlowControllerListener {

  /**
    * Called when an AMQP message is received on the link
    *
    * @param delivery     Proton delivery instance
    * @param message      Proton AMQP message
    */
  def onAcquire(delivery: ProtonDelivery, message: Message): Unit
}

/**
  * Provides message flow control
  *
  * @param receiver       AMQP receiver instance
  * @param listener       Listener for flow controller events
  */
private [streaming]
class AMQPFlowController(
        receiver: ProtonReceiver,
        listener: AMQPFlowControllerListener
     ) extends Logging {

  protected final val CREDITS_DEFAULT = 1000
  protected final val CREDITS_THRESHOLD = (CREDITS_DEFAULT * 50) / 100

  protected var count = 0
  protected var credits = 0

  // check on the receiver and listener instances
  if (Option(receiver).isEmpty)
    throw new IllegalArgumentException("The receiver instance cannot be null")

  if (Option(listener).isEmpty)
    throw new IllegalArgumentException("The listener instance cannot be null")

  /**
    * Open/start the flow controller activity
    */
  final def open(): Unit = {

    count = 0
    credits = CREDITS_DEFAULT

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

    // disable prefetch in order to use manual flow control
    receiver.setPrefetch(0)
    // grant the first bunch of credits
    receiver.flow(credits)

    receiver.open()
  }

  /**
    * Close/end the flow controller activity
    */
  final def close(): Unit = {

    // extension point before closing receiver
    beforeClose()

    if (Option(receiver).isDefined) {
      receiver.close()
    }
  }

  def beforeOpen(): Unit = { }

  def beforeClose(): Unit = { }

  protected def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    logDebug(s"Process delivery tag [${ new String(delivery.getTag()) }]")

    count += 1
    // raise the listener event in order to pass delivery and message
    listener.onAcquire(delivery, message)

    // issue credits if needed
    issueCredits()
  }

  /**
    * Check and issue credits
    */
  protected def issueCredits(): Unit = {

    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CREDITS_THRESHOLD) {

      val creditsToIssue = count
      logDebug(s"Flow: count ${count} >= ${credits - CREDITS_THRESHOLD} ... issuing ${creditsToIssue} credits")
      receiver.flow(creditsToIssue)
      count = 0
    }
  }
}

/**
  * AMQP flow controller implementation using asynchronous way with decoupling queue
  *
  * @param receiver     AMQP receiver instance
  * @param listener     Listener for flow controller events
  * @param maxRate      Max receiving rate
  * @param vertx        Vert.x instance used for timing feature
  */
private final class AMQPAsyncFlowController(
                     receiver: ProtonReceiver,
                     listener: AMQPFlowControllerListener,
                     maxRate: Long,
                     vertx: Vertx
                   ) extends AMQPFlowController(receiver, listener) with Handler[Long] {

  // queue for decoupling message handler by Vert.x with block generator add feature
  // in order to avoid blocking Vert.x event loop when message rate is higher than maximum
  var queue: mutable.Queue[(ProtonDelivery, Message)] = new mutable.Queue[(ProtonDelivery, Message)]()

  var last: Long = 0L

  // as defined in the RateLimiter class (from Google Guava library used by Spark)
  val permitsPerSecond: Double = maxRate.toDouble
  val stableIntervalMicros: Double = TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond

  var timerScheduled: Boolean = false

  override def beforeOpen(): Unit = {

    last = 0L
    timerScheduled = false
    queue.clear()

    logInfo(s"permitsPerSecond ${permitsPerSecond}, stableIntervalMicros ${TimeUnit.MICROSECONDS.toMillis(stableIntervalMicros.toLong)}")
  }

  override def beforeClose(): Unit = {

  }

  override def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    val now: Long = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

    // the sender rate is lower than the maximum specified,
    // process directly without enqueuing the message
    // NOTE : in-time message can be processed only if queue is empty
    //        in order to avoid "out of order" messages processing
    if ((now > last + stableIntervalMicros) && queue.isEmpty) {

      last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
      super.acquire(delivery, message)

    // the sender rate is higher than the maximum specified,
    // this message is arrived too quickly
    } else {

      logDebug(s"--> Enqueue delivery tag [${ new String(delivery.getTag()) }]")

      queue.enqueue(new Tuple2(delivery, message))

      // schedule timer for dequeuing messages
      scheduleTimer()
    }

  }

  /**
    * Schedule the Vert.x timer for handling the messages queue
    */
  private def scheduleTimer(): Unit = {

    // timer not already scheduled
    if (!timerScheduled) {

      var delay: Long = TimeUnit.MICROSECONDS.toMillis(stableIntervalMicros.toLong)
      // Vert.x timer resolution 1 ms
      if (delay == 0)
        delay = 1L

      logDebug(s"Timer scheduled every ${delay} ms")
      vertx.setTimer(delay, this)

      timerScheduled = true
    }
  }

  /**
    * Handler for the Vert.x timer scheduled for handling the messages queue
    *
    * @param timerId    Timer ID
    */
  override def handle(timerId: Long): Unit = {

    timerScheduled = false

    val t = queue.dequeue()

    logDebug(s"<-- Dequeue delivery tag [${ new String(t._1.getTag()) }]")

    last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
    super.acquire(t._1, t._2)

    // re-scheduled timer only if there is something in the queue to process
    if (!queue.isEmpty) {
      scheduleTimer()
    }

  }

}

/**
  * AMQP flow controller implementation using high resolution asynchronous way with decoupling queue
  *
  * @param receiver     AMQP receiver instance
  * @param listener     Listener for flow controller events
  * @param maxRate      Max receiving rate
  */
private final class AMQPHrAsyncFlowController(
                       receiver: ProtonReceiver,
                       listener: AMQPFlowControllerListener,
                       maxRate: Long
                     ) extends AMQPFlowController(receiver, listener) with Runnable {

  private final val MinStableIntervalMicros = 100.0

  // queue for decoupling message handler by Vert.x with block generator add feature
  // in order to avoid blocking Vert.x event loop when message rate is higher than maximum
  var queue: BlockingQueue[(ProtonDelivery, Message)] = new LinkedBlockingQueue[(ProtonDelivery, Message)]()

  var last: Long = 0L

  // as defined in the RateLimiter class (from Google Guava library used by Spark)
  val permitsPerSecond: Double = maxRate.toDouble

  val stableIntervalMicros: Double = {
    // with high permitsPerSecond (i.e. no max rate set), the division could give value near 0 us.
    // A minimum value for the scheduled executor at fixed rate is needed
    if (TimeUnit.SECONDS.toMicros(1L) < permitsPerSecond) MinStableIntervalMicros else TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond
  }

  // timer used for dequeing messages
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var scheduled: Option[ScheduledFuture[_]] = None

  override def beforeOpen(): Unit = {

    last = 0L
    queue.clear()

    logInfo(s"permitsPerSecond ${permitsPerSecond}, stableIntervalMicros ${stableIntervalMicros}")
  }

  override def beforeClose(): Unit = {

    scheduledExecutorService.shutdown()
    scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS)
    scheduled = None
  }

  override def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    val now: Long = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

    // queue empty, the running timer can be cancelled
    if (queue.isEmpty && scheduled.isDefined) {
      scheduled.get.cancel(false)
      scheduled = None
      logInfo(s"Timer cancelled")
    }

    // the sender rate is lower than the maximum specified,
    // process directly without enqueuing the message
    // NOTE : in-time message can be processed only if queue is empty
    //        in order to avoid "out of order" messages processing
    if ((now > last + stableIntervalMicros) && queue.isEmpty) {

      last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
      super.acquire(delivery, message)

    // the sender rate is higher than the maximum specified,
    // this message is arrived too quickly
    } else {

      logDebug(s"--> Enqueue delivery tag [${ new String(delivery.getTag()) }]")

      queue.put(new Tuple2(delivery, message))

      // schedule timer for dequeuing messages
      scheduleTimer()
    }
  }

  /**
    * Schedule the timer for handling the messages queue
    */
  private def scheduleTimer(): Unit = {

    // timer not already scheduled
    if (scheduled.isEmpty) {

      logDebug(s"Timer scheduled every ${stableIntervalMicros.toLong} us")
      scheduled = Option(scheduledExecutorService.scheduleWithFixedDelay(this, stableIntervalMicros.toLong, stableIntervalMicros.toLong, TimeUnit.MICROSECONDS))
    }
  }

  /**
    * Running scheduled timer for handling the messages queue
    */
  override def run(): Unit = {

    if (!queue.isEmpty) {

      val t = queue.take()

      logDebug(s"<-- Dequeue delivery tag [${new String(t._1.getTag())}]")

      last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
      super.acquire(t._1, t._2)
    }
  }

}
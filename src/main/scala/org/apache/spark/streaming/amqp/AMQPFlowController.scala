package org.apache.spark.streaming.amqp

import java.lang.Long
import java.util.concurrent.TimeUnit

import io.vertx.core.{Handler, Vertx}
import io.vertx.proton.{ProtonDelivery, ProtonMessageHandler, ProtonReceiver}
import org.apache.qpid.proton.message.Message
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.receiver.BlockGenerator

import scala.collection.mutable

/**
  * Provides message rate control with related throttling
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
abstract class AMQPFlowController(
                 blockGenerator: BlockGenerator,
                 receiver: ProtonReceiver
                 ) extends Logging {

  // check on the receiver and block generator instances
  if (receiver == null)
    throw new IllegalArgumentException("The receiver instance cannot be null")

  if (blockGenerator == null)
    throw new IllegalArgumentException("The block generator instance cannot be null")

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

    if (receiver != null) {
      receiver.close()
    }
  }

  def beforeOpen(): Unit = { }

  def beforeClose(): Unit = { }

  def acquire(delivery: ProtonDelivery, message: Message): Unit = { }
}

private final class AMQPAsyncFlowController(
                     vertx: Vertx,
                     blockGenerator: BlockGenerator,
                     receiver: ProtonReceiver
                   ) extends AMQPFlowController(blockGenerator, receiver) {

  private final val CreditsDefault = 10
  private final val CreditsThreshold = (CreditsDefault / 100) * 50

  var count = 0
  var credits = 0

  var queue: mutable.Queue[(ProtonDelivery, Message)] = new mutable.Queue[(ProtonDelivery, Message)]()

  var last: Long = 0L

  // as defined in the RateLimiter class (from Google Guava library used by Spark)
  val permitsPerSecond: Double = blockGenerator.getCurrentLimit.toDouble
  val stableIntervalMicros: Double = TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond

  val timerHandler: TimerHandler = new TimerHandler()
  var timerSet: Boolean = false

  override def beforeOpen(): Unit = {

    count = 0
    credits = CreditsDefault

    logInfo(s"permitsPerSecond ${permitsPerSecond}, stableIntervalMicros ${TimeUnit.MICROSECONDS.toMillis(stableIntervalMicros.toLong)}")

    // disable prefetch in order to use manual flow control
    receiver.setPrefetch(0)
    // grant the first bunch of credits
    receiver.flow(credits)

    super.beforeOpen()
  }

  override def beforeClose(): Unit = {

    super.beforeClose()
  }

  override def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    val now: Long = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

    // the sender rate is lower than the maximum specified,
    // the adding on blockGenerator should not block
    if (now > last + stableIntervalMicros) {

      // permit acquired, add message
      if (blockGenerator.isActive()) {

        // only AMQP message will be stored into BlockGenerator internal buffer;
        // delivery is passed as metadata to onAddData and saved here internally
        blockGenerator.addDataWithCallback(message, delivery)
      }

      count += 1
      last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

      // if the credits exhaustion is near, need to grant more credits
      if (count >= credits - CreditsThreshold) {
        logInfo(s"count ${count} ... issuing ${credits - CreditsThreshold} credits")
        receiver.flow(credits - CreditsThreshold)
        count = 0
      }

    // the sender rate is higher than the maximum specified,
    // this message is arrived too quickly
    } else {

      queue.enqueue(new Tuple2(delivery, message))

      // start timer for dequeuing messages
      if (!timerSet) {

        var delay: Long = TimeUnit.MICROSECONDS.toMillis(last + stableIntervalMicros.toLong - now)
        // Vert.x timer resolution 1 ms
        if (delay == 0)
          delay = 1L

        timerSet = true
        vertx.setTimer(delay, timerHandler)
      }

    }

    super.acquire(delivery, message)
  }

  class TimerHandler extends Handler[Long] {

    override def handle(event: Long): Unit = {

      logInfo("Dequeue")

      val t = queue.dequeue()
      // permit acquired, add message
      if (blockGenerator.isActive()) {

        // only AMQP message will be stored into BlockGenerator internal buffer;
        // delivery is passed as metadata to onAddData and saved here internally
        blockGenerator.addDataWithCallback(t._2, t._1)
      }

      count += 1
      last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

      if (!queue.isEmpty) {

        var delay: Long = stableIntervalMicros.toLong
        // Vert.x timer resolution 1 ms
        if (delay == 0)
          delay = 1L

        if (queue.size >= CreditsThreshold) {
          logInfo(s"queue.size ${queue.size} ... issuing ${credits - CreditsThreshold} credits")
          receiver.flow(credits - CreditsThreshold)
          count = 0
        }

        vertx.setTimer(delay, timerHandler)

      } else {
        timerSet = false
      }
    }
  }
}

private final class AMQPSyncFlowController(
                      blockGenerator: BlockGenerator,
                      receiver: ProtonReceiver
                    ) extends AMQPFlowController(blockGenerator, receiver) {

  private final val CreditsDefault = 200
  private final val CreditsThreshold = (CreditsDefault / 100) * 50

  var count = 0
  var credits = 0

  override def beforeOpen(): Unit = {

    count = 0
    credits = CreditsDefault

    // disable prefetch in order to use manual flow control
    receiver.setPrefetch(0)
    // grant the first bunch of credits
    receiver.flow(credits)

    super.beforeOpen()
  }

  override def beforeClose(): Unit = {

    super.beforeClose()
  }

  override def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    // permit acquired, add message
    if (blockGenerator.isActive()) {

      // only AMQP message will be stored into BlockGenerator internal buffer;
      // delivery is passed as metadata to onAddData and saved here internally
      blockGenerator.addDataWithCallback(message, delivery)
    }

    count += 1
    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {
      logInfo(s"count ${count} ... issuing ${credits - CreditsThreshold} credits")
      receiver.flow(credits - CreditsThreshold)
      count = 0
    }

    super.acquire(delivery, message)
  }
}

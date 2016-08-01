package org.apache.spark.streaming.amqp

import java.lang.Long
import java.util.concurrent._

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

/**
  * AMQP rate controller implementation using asynchronous way with decoupling queue
  *
  * @param vertx                Vert.x instance used for timing feature
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
private final class AMQPAsyncFlowController(
                     vertx: Vertx,
                     blockGenerator: BlockGenerator,
                     receiver: ProtonReceiver
                   ) extends AMQPFlowController(blockGenerator, receiver) with Handler[Long] {

  private final val CreditsDefault = 1000
  private final val CreditsThreshold = (CreditsDefault * 50) / 100

  var count = 0
  var credits = 0

  // queue for decoupling message handler by Vert.x with block generator add feature
  // in order to avoid blocking Vert.x event loop when message rate is higher than maximum
  var queue: mutable.Queue[(ProtonDelivery, Message)] = new mutable.Queue[(ProtonDelivery, Message)]()

  var last: Long = 0L

  // as defined in the RateLimiter class (from Google Guava library used by Spark)
  val permitsPerSecond: Double = blockGenerator.getCurrentLimit.toDouble
  val stableIntervalMicros: Double = TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond

  var timerScheduled: Boolean = false

  override def beforeOpen(): Unit = {

    count = 0
    credits = CreditsDefault
    last = 0L
    timerScheduled = false

    queue.clear()

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
    // NOTE : in-time message can be processed only if queue is empty
    //        in order to avoid "out of order" messages processing
    if ((now > last + stableIntervalMicros) && queue.isEmpty) {

      // add message and delivery to the block generator
      addMessageDelivery(delivery, message)

      // issue credits if needed
      issueCredits()

    // the sender rate is higher than the maximum specified,
    // this message is arrived too quickly
    } else {

      logInfo(s"--> Enqueue delivery tag [${ new String(delivery.getTag()) }]")

      queue.enqueue(new Tuple2(delivery, message))

      // schedule timer for dequeuing messages
      scheduleTimer()
    }

    super.acquire(delivery, message)
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

      logInfo(s"Timer scheduled every ${delay} ms")
      vertx.setTimer(delay, this)

      timerScheduled = true
    }
  }

  /**
    * Check and issue credits
    */
  private def issueCredits(): Unit = {

    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {

      val creditsToIssue = count
      logInfo(s"Flow: queue.size ${queue.size}, count ${count} >= ${credits - CreditsThreshold} ... issuing ${creditsToIssue} credits")
      receiver.flow(creditsToIssue)
      count = 0
    }
  }

  /**
    * Add message and delivery to the block generator
    * @param delivery       Proton delivery instance
    * @param message        Proton AMQP message
    */
  private def addMessageDelivery(delivery: ProtonDelivery, message: Message): Unit = {

    logInfo(s"Process delivery tag [${ new String(delivery.getTag()) }]")

    // only AMQP message will be stored into BlockGenerator internal buffer;
    // delivery is passed as metadata to onAddData and saved here internally
    blockGenerator.addDataWithCallback(message, delivery)

    count += 1
    last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
  }
  /**
    * Handler for the Vert.x timer scheduled for handling the messages queue
    * @param timerId    Timer ID
    */
  override def handle(timerId: Long): Unit = {

    timerScheduled = false

    val t = queue.dequeue()

    logInfo(s"<-- Dequeue delivery tag [${ new String(t._1.getTag()) }]")

    // add message and delivery to the block generator
    addMessageDelivery(t._1, t._2)

    // issue credits if needed
    issueCredits()

    // re-scheduled timer only if there is something in the queue to process
    if (!queue.isEmpty) {
      scheduleTimer()
    }

  }

}

/**
  * AMQP rate controller implementation using synchronous way blocking on block generator
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
private final class AMQPSyncFlowController(
                      blockGenerator: BlockGenerator,
                      receiver: ProtonReceiver
                    ) extends AMQPFlowController(blockGenerator, receiver) {

  private final val CreditsDefault = 1000
  private final val CreditsThreshold = (CreditsDefault * 50) / 100

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

    logInfo(s"Process delivery tag [${ new String(delivery.getTag()) }]")

    // only AMQP message will be stored into BlockGenerator internal buffer;
    // delivery is passed as metadata to onAddData and saved here internally
    blockGenerator.addDataWithCallback(message, delivery)

    count += 1
    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {

      val creditsToIssue = count
      logInfo(s"count ${count} >= ${credits - CreditsThreshold} ... issuing ${creditsToIssue} credits")
      receiver.flow(creditsToIssue)
      count = 0
    }

    super.acquire(delivery, message)
  }
}

/**
  * AMQP rate controller implementation using high resolution asynchronous way with decoupling queue
  *
  * @param blockGenerator       BlockGenerator instance used for storing messages
  * @param receiver             AMQP receiver instance
  */
private final class AMQPHrAsyncFlowController(
                       blockGenerator: BlockGenerator,
                       receiver: ProtonReceiver
                     ) extends AMQPFlowController(blockGenerator, receiver) with Runnable {

  private final val CreditsDefault = 1000
  private final val CreditsThreshold = (CreditsDefault * 50) / 100
  private final val MinStableIntervalMicros = 100.0

  var count = 0
  var credits = 0

  // queue for decoupling message handler by Vert.x with block generator add feature
  // in order to avoid blocking Vert.x event loop when message rate is higher than maximum
  var queue: BlockingQueue[(ProtonDelivery, Message)] = new LinkedBlockingQueue[(ProtonDelivery, Message)]()

  var last: Long = 0L

  // as defined in the RateLimiter class (from Google Guava library used by Spark)
  val permitsPerSecond: Double = blockGenerator.getCurrentLimit.toDouble

  val stableIntervalMicros: Double = {
    // with high permitsPerSecond (i.e. no max rate set), the division could give value near 0 us.
    // A minimum value for the scheduled executor at fixed rate is needed
    if (TimeUnit.SECONDS.toMicros(1L) < permitsPerSecond) MinStableIntervalMicros else TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond
  }

  // timer used for dequeing messages
  private val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var scheduled: ScheduledFuture[_] = null

  override def beforeOpen(): Unit = {

    count = 0
    credits = CreditsDefault
    last = 0L

    queue.clear()

    logInfo(s"permitsPerSecond ${permitsPerSecond}, stableIntervalMicros ${stableIntervalMicros}")

    // disable prefetch in order to use manual flow control
    receiver.setPrefetch(0)
    // grant the first bunch of credits
    receiver.flow(credits)

    super.beforeOpen()
  }

  override def beforeClose(): Unit = {

    scheduledExecutorService.shutdown()
    scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS)
    scheduled = null

    super.beforeClose()
  }

  override def acquire(delivery: ProtonDelivery, message: Message): Unit = {

    val now: Long = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)

    // queue empyt, the running timer can be cancelled
    if ((queue.isEmpty) && (scheduled != null)) {
      scheduled.cancel(false)
      scheduled = null
      logInfo(s"Timer cancelled")
    }

    // the sender rate is lower than the maximum specified,
    // the adding on blockGenerator should not block
    // NOTE : in-time message can be processed only if queue is empty
    //        in order to avoid "out of order" messages processing
    if ((now > last + stableIntervalMicros) && queue.isEmpty) {

      // add message and delivery to the block generator
      addMessageDelivery(delivery, message)

      // issue credits if needed
      issueCredits()

    // the sender rate is higher than the maximum specified,
    // this message is arrived too quickly
    } else {

      logInfo(s"--> Enqueue delivery tag [${ new String(delivery.getTag()) }]")

      queue.put(new Tuple2(delivery, message))

      // schedule timer for dequeuing messages
      scheduleTimer()
    }

    super.acquire(delivery, message)
  }

  /**
    * Schedule the timer for handling the messages queue
    */
  private def scheduleTimer(): Unit = {

    // timer not already scheduled
    if (scheduled == null) {

      logInfo(s"Timer scheduled every ${stableIntervalMicros.toLong} us")
      scheduled = scheduledExecutorService.scheduleWithFixedDelay(this, stableIntervalMicros.toLong, stableIntervalMicros.toLong, TimeUnit.MICROSECONDS)
    }
  }

  /**
    * Check and issue credits
    */
  private def issueCredits(): Unit = {

    // if the credits exhaustion is near, need to grant more credits
    if (count >= credits - CreditsThreshold) {

      val creditsToIssue = count
      logInfo(s"Flow: queue.size ${queue.size}, count ${count} >= ${credits - CreditsThreshold} ... issuing ${creditsToIssue} credits")
      receiver.flow(creditsToIssue)
      count = 0
    }
  }

  /**
    * Add message and delivery to the block generator
    *
    * @param delivery       Proton delivery instance
    * @param message        Proton AMQP message
    */
  private def addMessageDelivery(delivery: ProtonDelivery, message: Message): Unit = {

    logInfo(s"Process delivery tag [${ new String(delivery.getTag()) }]")

    // only AMQP message will be stored into BlockGenerator internal buffer;
    // delivery is passed as metadata to onAddData and saved here internally
    blockGenerator.addDataWithCallback(message, delivery)

    count += 1
    last = TimeUnit.NANOSECONDS.toMicros(System.nanoTime)
  }

  /**
    * Running scheduled timer for handling the messages queue
    */
  override def run(): Unit = {

    if (!queue.isEmpty) {

      val t = queue.take()

      logInfo(s"<-- Dequeue delivery tag [${new String(t._1.getTag())}]")

      // add message and delivery to the block generator
      addMessageDelivery(t._1, t._2)

      // issue credits if needed
      issueCredits()
    }
  }

}
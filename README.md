# AMQP connector for Spark Streaming

This project provides an [AMQP](https://www.amqp.org/) (Advanced Message Queuing Protocol) connector for [Apache Spark Streaming](http://spark.apache.org/streaming/) in order to ingest data as a stream from all possible AMQP based sources like :

* a pure AMQP server, which exposes a sender node for sending messages in a peer to peer fashion with the connector
* a messaging broker, which supports AMQP protocol and provide "store and forward" mechanism from queues and topics/subscriptions for the connector
* a router network, which provides AMQP routing with direct messaging or link routing

The implementation offers the following receivers :

* a non reliable receiver which doesn't settle the messages received from the AMQP sender
* a reliable receiver which settles messages received from the AMQP sender only after storing them reliably in the Spark cluster (it uses the checkpoint and write ahead log features)

The stream doesn't provide the received AMQP messages directly as elements of the RDDs micro batches but from the driver it's possible to pass a converter function in order to convert each message in the desidered format; it will be the type of the elements inside the RDDs micro batches. Two built in message converter functions are provided (as sample) :

* a converter which returns only the AMQP message body in a custom serializable type T
* a converter which returns the JSON string representation of the entire AMQP message

## Project References

### Maven

```
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-amqp_2.11</artifactId>
    <version>0.1.0</version>
</dependency>
```

### SBT

```
libraryDependencies += "org.apache.spark" %% "spark-streaming-amqp_2.11" % "0.1.0"
```

## Example

TBD

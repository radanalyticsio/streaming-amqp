# AMQP connector for Spark Streaming

This project provides an [AMQP](https://www.amqp.org/)(Advanced Message Queuing Protocol) connector for Apache Spark Streaming in order to ingest data as a stream from all possible AMQP based products like :

* a pure server
* a messaging broker
* a router network

The implementation offers following receivers :

* a non reliable receiver which doesn't settle the messages received from the AMQP sender 
* a reliable receiver which settles messages received from the AMQP after only after storing them reliably in the Spark cluster (it uses the checkpoint and write ahead log features)

The stream doesn't provide the received AMQP messages directly but from the driver it's possible to pass a converter function in order to convert each message in the desidered format. Two built in message converter functions are provided :

* a converter which returns only the AMQP message body in a custom type T
* a converter which returns the JSON string representation of the entire AMQP message

## Project References

TBD

## Example

TBD

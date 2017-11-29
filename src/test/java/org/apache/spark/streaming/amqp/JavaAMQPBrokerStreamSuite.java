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

package org.apache.spark.streaming.amqp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.proton.message.Message;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.util.*;
import java.util.Map.Entry;

/**
 * Java test suite for the AMQP input stream
 */
public class JavaAMQPBrokerStreamSuite {

    private Duration batchDuration = new Duration(1000);
    private String master = "local[2]";
    private String appName = this.getClass().getSimpleName();
    private String address = "my_address";
    private String checkpointDir = "/tmp/spark-streaming-amqp-tests";

    private SparkConf conf = null;
    private JavaStreamingContext jssc = null;
    private AMQPTestUtils amqpTestUtils = null;

    @Before
    public void setup() {

        this.conf = new SparkConf().setMaster(this.master).setAppName(this.appName);
        conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        this.jssc = new JavaStreamingContext(this.conf, this.batchDuration);
        this.jssc.checkpoint(checkpointDir);

        this.amqpTestUtils = new AMQPTestUtils();
        this.amqpTestUtils.setup();

        this.amqpTestUtils.startBroker();
    }

    @After
    public void teardown() {

        this.amqpTestUtils.stopBroker();

        if (this.jssc != null) {
            this.jssc.stop();
        }

        if (this.amqpTestUtils != null) {
            this.amqpTestUtils.teardown();
        }
    }

    @Test
    public void testAMQPReceiveSimpleBodyString() {

        Function<Message, Option<String>> converter = new JavaAMQPBodyFunction<>();

        String sendMessage = "Spark Streaming & AMQP";
        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        List<String> receivedMessage = new ArrayList<>();
        receiveStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendSimpleMessage(address, sendMessage);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receivedMessage.get(0).equals(sendMessage));

        jssc.stop();
    }

    @Test
    public void testAMQPReceiveListBody() {

        Function<Message, Option<String>> converter = new JavaAMQPJsonFunction();

        List<Object> list = new ArrayList<>();
        list.add("a string");
        list.add(1);
        list.add(2);

        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        JavaDStream<String> listStream = receiveStream.map(jsonMsg -> {

            ObjectMapper mapper = new ObjectMapper();

            List<String> listFinal = new ArrayList<>();

            // get an iterator on "section" that is actually an array
            Iterator<JsonNode> iterator = mapper.readTree(jsonMsg).get("body").get("section").elements();
            while(iterator.hasNext()) {
                listFinal.add(iterator.next().asText());
            }

            return StringUtils.join(listFinal, ',');
        });

        List<String> receivedMessage = new ArrayList<>();
        listStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendComplexMessage(address, list);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receivedMessage.get(0).equals(StringUtils.join(list, ',')));

        jssc.stop();
    }

    @Test
    public void testAMQPReceiveMapBody() {

        Function<Message, Option<String>> converter = new JavaAMQPJsonFunction();

        Map<Object, Object> map = new HashMap<>();
        map.put("field_a", "a string");
        map.put("field_b", 1);

        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        JavaDStream<String> mapStream = receiveStream.map(jsonMsg -> {

            ObjectMapper mapper = new ObjectMapper();

            List<String> listFinal = new ArrayList<>();

            // get an iterator on all fields of "section" that is actually a map
            Iterator<Entry<String, JsonNode>> iterator = mapper.readTree(jsonMsg).get("body").get("section").fields();
            while(iterator.hasNext()) {
                Entry<String, JsonNode> entry = iterator.next();
                listFinal.add(entry.getKey() + "=" + entry.getValue().asText());
            }

            return StringUtils.join(listFinal, ',');
        });

        List<String> receivedMessage = new ArrayList<>();
        mapStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendComplexMessage(address, map);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        StringBuilder sbuilder = new StringBuilder();
        for (Entry<Object, Object> entry: map.entrySet()) {
            sbuilder.append(entry.getKey() + "=" + entry.getValue() + ",");
        }
        sbuilder.deleteCharAt(sbuilder.length() - 1);

        assert(receivedMessage.get(0).equals(sbuilder.toString()));

        jssc.stop();
    }

    @Test
    public void testAMQPReceiveArrayBody() {

        Function<Message, Option<String>> converter = new JavaAMQPJsonFunction();

        Object[] array = { 1, 2 };

        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        JavaDStream<String> listStream = receiveStream.map(jsonMsg -> {

            ObjectMapper mapper = new ObjectMapper();

            List<String> listFinal = new ArrayList<>();

            // get an iterator on "section" that is actually an array
            Iterator<JsonNode> iterator = mapper.readTree(jsonMsg).get("body").get("section").elements();
            while(iterator.hasNext()) {
                listFinal.add(iterator.next().asText());
            }

            return StringUtils.join(listFinal, ',');
        });

        List<String> receivedMessage = new ArrayList<>();
        listStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendComplexMessage(address, array);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receivedMessage.get(0).equals(StringUtils.join(array, ',')));

        jssc.stop();
    }

    @Test
    public void testAMQPReceiveBinaryBody() {

        Function<Message, Option<String>> converter = new JavaAMQPJsonFunction();

        String sendMessage = "Spark Streaming & AMQP";
        JavaReceiverInputDStream<String>  receiveStream =
                AMQPUtils.createStream(this.jssc,
                        this.amqpTestUtils.host(),
                        this.amqpTestUtils.port(),
                        this.amqpTestUtils.username(),
                        this.amqpTestUtils.password(),
                        this.address, converter, StorageLevel.MEMORY_ONLY());

        JavaDStream<String> binaryStream = receiveStream.map(jsonMsg -> {

            ObjectMapper mapper = new ObjectMapper();

            String body = new String(Base64.getDecoder().decode(mapper.readTree(jsonMsg).get("body").get("section").asText()));

            return body;
        });

        List<String> receivedMessage = new ArrayList<>();
        binaryStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                receivedMessage.add(rdd.first());
            }
        });

        jssc.start();

        this.amqpTestUtils.sendBinaryMessage(address, sendMessage.getBytes());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assert(receivedMessage.get(0).equals(sendMessage));

        jssc.stop();
    }

}

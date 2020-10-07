/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.mqtt.common;

import io.moquette.proto.messages.AbstractMessage;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.mqtt.ConsumeMQTT;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_FALSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class TestConsumeMqttCommon {

    public int PUBLISH_WAIT_MS = 1000;
    public static final String THIS_IS_NOT_JSON = "ThisIsNotAJSON";

    public Server MQTT_server;
    public TestRunner testRunner;
    public String broker;

    public abstract void internalPublish(PublishMessage publishMessage);

    @Test
    public void testClientIDConfiguration() {
        TestRunner runner = TestRunners.newTestRunner(ConsumeMQTT.class);
        runner.setProperty(ConsumeMQTT.PROP_BROKER_URI, "tcp://localhost:1883");
        runner.setProperty(ConsumeMQTT.PROP_CLIENTID, "TestClient");
        runner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, "testTopic");
        runner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "100");
        runner.assertValid();

        runner.setProperty(ConsumeMQTT.PROP_GROUPID, "group");
        runner.assertNotValid();

        runner.setProperty(ConsumeMQTT.PROP_CLIENTID, "${hostname()}");
        runner.assertValid();

        runner.removeProperty(ConsumeMQTT.PROP_CLIENTID);
        runner.assertValid();
    }

    @Test
    public void testLastWillConfig() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_MESSAGE, "lastWill message");
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_TOPIC, "lastWill topic");
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_QOS, "1");
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_RETAIN, "false");
        testRunner.assertValid();
    }


    @Test
    public void testQoS2() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS2NotCleanSession() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");
        testRunner.setProperty(ConsumeMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage);

        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }


    @Test
    public void testQoS1() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "1");

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() > 0);
        assertProvenanceEvents(flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "1");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS1NotCleanSession() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "1");
        testRunner.setProperty(ConsumeMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage);

        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() > 0);
        assertProvenanceEvents(flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "1");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS0() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "0");

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() < 2);
        assertProvenanceEvents(flowFiles.size());

        if(flowFiles.size() == 1) {
            MockFlowFile flowFile = flowFiles.get(0);

            flowFile.assertContentEquals("testMessage");
            flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
            flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
            flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "0");
            flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
            flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
        }
    }

    @Test
    public void testOnStoppedFinish() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        MqttMessage innerMessage = new MqttMessage();
        innerMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()).array());
        innerMessage.setQos(2);
        MQTTQueueMessage testMessage = new MQTTQueueMessage("testTopic", innerMessage);

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.processSessionFactory = testRunner.getProcessSessionFactory();

        Field f = ConsumeMQTT.class.getDeclaredField("mqttQueue");
        f.setAccessible(true);
        LinkedBlockingQueue<MQTTQueueMessage> queue = (LinkedBlockingQueue<MQTTQueueMessage>) f.get(consumeMQTT);
        queue.add(testMessage);

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());
        consumeMQTT.onStopped(testRunner.getProcessContext());

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testResizeBuffer() throws Exception {
        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "2");

        testRunner.assertValid();

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        internalPublish(testMessage);
        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);
        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "1");
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "3");
        testRunner.assertValid();

        testRunner.run(1);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 2);
        assertProvenanceEvents(2);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testConsumeRecordsWithAddedFields() throws Exception {
        testRunner.setProperty(ConsumeMQTT.RECORD_READER, "record-reader");
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, "record-writer");

        final JsonTreeReader jsonReader = new JsonTreeReader();
        testRunner.addControllerService("record-reader", jsonReader);
        testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        testRunner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", jsonWriter);
        testRunner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        testRunner.enableControllerService(jsonWriter);

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("{\"name\":\"Apache NiFi\"}".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        testMessage.setRetainFlag(false);

        PublishMessage badMessage = new PublishMessage();
        badMessage.setPayload(ByteBuffer.wrap(THIS_IS_NOT_JSON.getBytes()));
        badMessage.setTopicName("testTopic");
        badMessage.setDupFlag(false);
        badMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        badMessage.setRetainFlag(false);

        internalPublish(testMessage);
        internalPublish(badMessage);
        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() == 1);
        assertEquals("[{\"name\":\"Apache NiFi\",\"_topic\":\"testTopic\",\"_qos\":0,\"_isDuplicate\":false,\"_isRetained\":false},"
                + "{\"name\":\"Apache NiFi\",\"_topic\":\"testTopic\",\"_qos\":0,\"_isDuplicate\":false,\"_isRetained\":false}]",
                new String(flowFiles.get(0).toByteArray()));

        List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertTrue(badFlowFiles.size() == 1);
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.get(0).toByteArray()));

        // clean runner by removing records reader/writer
        testRunner.removeProperty(ConsumeMQTT.RECORD_READER);
        testRunner.removeProperty(ConsumeMQTT.RECORD_WRITER);
    }

    @Test
    public void testConsumeDemarcator() throws Exception {
        testRunner.setProperty(ConsumeMQTT.MESSAGE_DEMARCATOR, "\\n");
        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("{\"name\":\"Apache NiFi\"}".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        testMessage.setRetainFlag(false);

        PublishMessage badMessage = new PublishMessage();
        badMessage.setPayload(ByteBuffer.wrap(THIS_IS_NOT_JSON.getBytes()));
        badMessage.setTopicName("testTopic");
        badMessage.setDupFlag(false);
        badMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        badMessage.setRetainFlag(false);

        internalPublish(testMessage);
        internalPublish(badMessage);
        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);
        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertEquals(flowFiles.size(), 1);
        assertEquals("{\"name\":\"Apache NiFi\"}\\n"
                + THIS_IS_NOT_JSON + "\\n"
                + "{\"name\":\"Apache NiFi\"}\\n",
                new String(flowFiles.get(0).toByteArray()));

        List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertTrue(badFlowFiles.size() == 0);

        // clean runner by removing message demarcator
        testRunner.removeProperty(ConsumeMQTT.MESSAGE_DEMARCATOR);
    }

    @Test
    public void testConsumeRecordsWithoutAddedFields() throws Exception {
        testRunner.setProperty(ConsumeMQTT.RECORD_READER, "record-reader");
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, "record-writer");
        testRunner.setProperty(ConsumeMQTT.ADD_ATTRIBUTES_AS_FIELDS, "false");

        final JsonTreeReader jsonReader = new JsonTreeReader();
        testRunner.addControllerService("record-reader", jsonReader);
        testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        testRunner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", jsonWriter);
        testRunner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        testRunner.enableControllerService(jsonWriter);

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("{\"name\":\"Apache NiFi\"}".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        testMessage.setRetainFlag(false);

        PublishMessage badMessage = new PublishMessage();
        badMessage.setPayload(ByteBuffer.wrap(THIS_IS_NOT_JSON.getBytes()));
        badMessage.setTopicName("testTopic");
        badMessage.setDupFlag(false);
        badMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        badMessage.setRetainFlag(false);

        internalPublish(testMessage);
        internalPublish(badMessage);
        internalPublish(testMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() == 1);
        assertEquals("[{\"name\":\"Apache NiFi\"},{\"name\":\"Apache NiFi\"}]", new String(flowFiles.get(0).toByteArray()));

        List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertTrue(badFlowFiles.size() == 1);
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.get(0).toByteArray()));

        // clean runner by removing records reader/writer
        testRunner.removeProperty(ConsumeMQTT.RECORD_READER);
        testRunner.removeProperty(ConsumeMQTT.RECORD_WRITER);
    }

    @Test
    public void testConsumeRecordsOnlyBadData() throws Exception {
        testRunner.setProperty(ConsumeMQTT.RECORD_READER, "record-reader");
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, "record-writer");
        testRunner.setProperty(ConsumeMQTT.ADD_ATTRIBUTES_AS_FIELDS, "false");

        final JsonTreeReader jsonReader = new JsonTreeReader();
        testRunner.addControllerService("record-reader", jsonReader);
        testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        testRunner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", jsonWriter);
        testRunner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        testRunner.enableControllerService(jsonWriter);

        testRunner.assertValid();

        ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        PublishMessage badMessage = new PublishMessage();
        badMessage.setPayload(ByteBuffer.wrap(THIS_IS_NOT_JSON.getBytes()));
        badMessage.setTopicName("testTopic");
        badMessage.setDupFlag(false);
        badMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        badMessage.setRetainFlag(false);

        internalPublish(badMessage);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertTrue(badFlowFiles.size() == 1);
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.get(0).toByteArray()));

        // clean runner by removing records reader/writer
        testRunner.removeProperty(ConsumeMQTT.RECORD_READER);
        testRunner.removeProperty(ConsumeMQTT.RECORD_WRITER);
    }

    private static boolean isConnected(AbstractMQTTProcessor processor) throws NoSuchFieldException, IllegalAccessException {
        Field f = AbstractMQTTProcessor.class.getDeclaredField("mqttClient");
        f.setAccessible(true);
        IMqttClient mqttClient = (IMqttClient) f.get(processor);
        return mqttClient.isConnected();
    }


    public static void reconnect(ConsumeMQTT processor, ProcessContext context) throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Method method = ConsumeMQTT.class.getDeclaredMethod("initializeClient", ProcessContext.class);
        method.setAccessible(true);
        method.invoke(processor, context);
    }

    public static BlockingQueue<MQTTQueueMessage> getMqttQueue(ConsumeMQTT consumeMQTT) throws IllegalAccessException, NoSuchFieldException {
        Field mqttQueueField = ConsumeMQTT.class.getDeclaredField("mqttQueue");
        mqttQueueField.setAccessible(true);
        return (BlockingQueue<MQTTQueueMessage>) mqttQueueField.get(consumeMQTT);
    }

    public static void transferQueue(ConsumeMQTT consumeMQTT, ProcessSession session) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method transferQueue = ConsumeMQTT.class.getDeclaredMethod("transferQueue", ProcessSession.class);
        transferQueue.setAccessible(true);
        transferQueue.invoke(consumeMQTT, session);
    }

    private void assertProvenanceEvents(int count){
        List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertNotNull(provenanceEvents);
        assertEquals(count, provenanceEvents.size());
        if (count > 0) {
            assertEquals(ProvenanceEventType.RECEIVE, provenanceEvents.get(0).getEventType());
        }
    }
}

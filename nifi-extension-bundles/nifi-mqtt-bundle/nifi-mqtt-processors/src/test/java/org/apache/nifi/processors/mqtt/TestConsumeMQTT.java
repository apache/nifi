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

package org.apache.nifi.processors.mqtt;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.mqtt.common.AbstractMQTTProcessor;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttTestClient;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_FALSE;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtil.createJsonRecordSetReaderService;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtil.createJsonRecordSetWriterService;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsumeMQTT {

    private static final int PUBLISH_WAIT_MS = 0;
    private static final String THIS_IS_NOT_JSON = "ThisIsNotAJSON";
    private static final String BROKER_URI = "tcp://localhost:1883";
    private static final String SSL_BROKER_URI = "ssl://localhost:8883";
    private static final String WS_BROKER_URI = "ws://localhost:15675/ws";
    private static final String WSS_BROKER_URI = "wss://localhost:15676/ws";
    private static final String CLUSTERED_BROKER_URI = "tcp://localhost:1883,tcp://localhost:1884";
    private static final String SSL_CLUSTERED_BROKER_URI = "ssl://localhost:1883,ssl://localhost:1884";
    private static final String INVALID_BROKER_URI = "http://localhost:1883";
    private static final String INVALID_CLUSTERED_BROKER_URI = "ssl://localhost:1883,tcp://localhost:1884";
    private static final String CLIENT_ID = "TestClient";
    private static final String TOPIC_NAME = "test/topic";
    private static final String INTERNAL_QUEUE_SIZE = "100";

    private static final String STRING_MESSAGE = "testMessage";
    private static final String JSON_PAYLOAD = "{\"name\":\"Apache NiFi\"}";

    private static final int AT_MOST_ONCE = 0;
    private static final int AT_LEAST_ONCE = 1;
    private static final int EXACTLY_ONCE = 2;

    private MqttTestClient mqttTestClient;
    private TestRunner testRunner;

    @AfterEach
    public void cleanup() {
        testRunner = null;
        mqttTestClient = null;
    }

    @Test
    public void testClientIDConfiguration() {
        testRunner = initializeTestRunner();
        testRunner.assertValid();

        testRunner.setProperty(ConsumeMQTT.PROP_GROUPID, "group");
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, "${hostname()}");
        testRunner.assertValid();

        testRunner.removeProperty(ConsumeMQTT.PROP_CLIENTID);
        testRunner.assertValid();
    }

    @Test
    public void testLastWillConfig() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_MESSAGE, "lastWill message");
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeMQTT.PROP_LAST_WILL_TOPIC, "lastWill topic");
        testRunner.assertValid();
    }

    @Test
    public void testBrokerUriConfig() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, INVALID_BROKER_URI);
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, INVALID_CLUSTERED_BROKER_URI);
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, BROKER_URI);
        testRunner.assertValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, CLUSTERED_BROKER_URI);
        testRunner.assertValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, WS_BROKER_URI);
        testRunner.assertValid();
    }

    @Test
    public void testSSLBrokerUriRequiresSSLContextServiceConfig() throws InitializationException {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, SSL_BROKER_URI);
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, SSL_CLUSTERED_BROKER_URI);
        testRunner.assertNotValid();

        final String identifier = addSSLContextService(testRunner);
        testRunner.setProperty(ConsumeMQTT.PROP_SSL_CONTEXT_SERVICE, identifier);
        testRunner.assertValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, SSL_BROKER_URI);
        testRunner.assertValid();

        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, WSS_BROKER_URI);
        testRunner.assertValid();
    }

    @Test
    public void testRecordAndDemarcatorConfigurationTogetherIsInvalid() throws InitializationException {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(ConsumeMQTT.MESSAGE_DEMARCATOR, "\n");

        testRunner.assertNotValid();
    }

    @Test
    public void testQoS2() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(STRING_MESSAGE, EXACTLY_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS2NotCleanSession() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");
        testRunner.setProperty(ConsumeMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        publishMessage(STRING_MESSAGE, EXACTLY_ONCE);

        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS1() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "1");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(STRING_MESSAGE, AT_LEAST_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertFalse(flowFiles.isEmpty());
        assertProvenanceEvents(flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "1");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS1NotCleanSession() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "1");
        testRunner.setProperty(ConsumeMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        publishMessage(STRING_MESSAGE, AT_LEAST_ONCE);

        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertFalse(flowFiles.isEmpty());
        assertProvenanceEvents(flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "1");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testQoS0() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "0");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(STRING_MESSAGE, AT_MOST_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertTrue(flowFiles.size() < 2);
        assertProvenanceEvents(flowFiles.size());

        if (flowFiles.size() == 1) {
            MockFlowFile flowFile = flowFiles.getFirst();

            flowFile.assertContentEquals("testMessage");
            flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
            flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
            flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "0");
            flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
            flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
        }
    }

    @Test
    public void testOnStoppedFinish() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        final byte[] content = ByteBuffer.wrap("testMessage".getBytes()).array();
        final ReceivedMqttMessage testMessage = new ReceivedMqttMessage(content, 2, false, TOPIC_NAME);

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        consumeMQTT.processSessionFactory = testRunner.getProcessSessionFactory();

        final Field f = ConsumeMQTT.class.getDeclaredField("mqttQueue");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        final BlockingQueue<ReceivedMqttMessage> queue = (BlockingQueue<ReceivedMqttMessage>) f.get(consumeMQTT);
        queue.add(testMessage);

        consumeMQTT.onUnscheduled(testRunner.getProcessContext());
        consumeMQTT.onStopped(testRunner.getProcessContext());

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 1);
        assertProvenanceEvents(1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testResizeBuffer() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.PROP_QOS, "2");
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "2");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(STRING_MESSAGE, EXACTLY_ONCE);
        publishMessage(STRING_MESSAGE, EXACTLY_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);
        consumeMQTT.onUnscheduled(testRunner.getProcessContext());

        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "1");
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, "3");
        testRunner.assertValid();

        testRunner.run(1);

        testRunner.assertTransferCount(ConsumeMQTT.REL_MESSAGE, 2);
        assertProvenanceEvents(2);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, BROKER_URI);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, TOPIC_NAME);
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testConsumeRecordsWithAddedFields() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(JSON_PAYLOAD, AT_MOST_ONCE);
        publishMessage(THIS_IS_NOT_JSON, AT_MOST_ONCE);
        publishMessage(JSON_PAYLOAD, AT_MOST_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertEquals(1, flowFiles.size());
        assertEquals("[{\"name\":\"Apache NiFi\",\"_topic\":\"test/topic\",\"_topicSegments\":[\"test\",\"topic\"],\"_qos\":0,\"_isDuplicate\":false,\"_isRetained\":false},"
                        + "{\"name\":\"Apache NiFi\",\"_topic\":\"test/topic\",\"_topicSegments\":[\"test\",\"topic\"],\"_qos\":0,\"_isDuplicate\":false,\"_isRetained\":false}]",
                new String(flowFiles.getFirst().toByteArray()));

        final List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertEquals(1, badFlowFiles.size());
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.getFirst().toByteArray()));
    }

    @Test
    public void testConsumeDemarcator() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.MESSAGE_DEMARCATOR, "\\n");
        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(JSON_PAYLOAD, AT_MOST_ONCE);
        publishMessage(THIS_IS_NOT_JSON, AT_MOST_ONCE);
        publishMessage(JSON_PAYLOAD, AT_MOST_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);
        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertEquals(flowFiles.size(), 1);
        assertEquals("{\"name\":\"Apache NiFi\"}\\n"
                        + THIS_IS_NOT_JSON + "\\n"
                        + "{\"name\":\"Apache NiFi\"}",
                new String(flowFiles.getFirst().toByteArray()));

        final List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertEquals(0, badFlowFiles.size());
    }

    @Test
    public void testConsumeRecordsWithoutAddedFields() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(ConsumeMQTT.ADD_ATTRIBUTES_AS_FIELDS, "false");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(JSON_PAYLOAD, AT_LEAST_ONCE);
        publishMessage(THIS_IS_NOT_JSON, AT_LEAST_ONCE);
        publishMessage(JSON_PAYLOAD, AT_LEAST_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_MESSAGE);
        assertEquals(1, flowFiles.size());
        assertEquals("[{\"name\":\"Apache NiFi\"},{\"name\":\"Apache NiFi\"}]", new String(flowFiles.getFirst().toByteArray()));

        final List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertEquals(1, badFlowFiles.size());
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.getFirst().toByteArray()));
    }

    @Test
    public void testConsumeRecordsOnlyBadData() throws Exception {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(ConsumeMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(ConsumeMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(ConsumeMQTT.ADD_ATTRIBUTES_AS_FIELDS, "false");

        testRunner.assertValid();

        final ConsumeMQTT consumeMQTT = (ConsumeMQTT) testRunner.getProcessor();
        consumeMQTT.onScheduled(testRunner.getProcessContext());
        reconnect(consumeMQTT, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(consumeMQTT));

        publishMessage(THIS_IS_NOT_JSON, EXACTLY_ONCE);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        final List<MockFlowFile> badFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeMQTT.REL_PARSE_FAILURE);
        assertEquals(1, badFlowFiles.size());
        assertEquals(THIS_IS_NOT_JSON, new String(badFlowFiles.getFirst().toByteArray()));
    }

    @Test
    public void testSslContextService() throws InitializationException {
        testRunner = initializeTestRunner();
        testRunner.setEnvironmentVariableValue("brokerURI",  "ssl://localhost:8883");
        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, "${brokerURI}");

        final String identifier = addSSLContextService(testRunner);
        testRunner.setProperty(ConsumeMQTT.PROP_SSL_CONTEXT_SERVICE, identifier);

        final ConsumeMQTT processor = (ConsumeMQTT) testRunner.getProcessor();
        processor.onScheduled(testRunner.getProcessContext());
    }

    @Test
    public void testMessageNotConsumedOnCommitFail() throws NoSuchFieldException, IllegalAccessException {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Subscriber);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.run(1, false);
        final ConsumeMQTT processor = (ConsumeMQTT) testRunner.getProcessor();
        final ReceivedMqttMessage mock = mock(ReceivedMqttMessage.class);
        when(mock.getPayload()).thenReturn(new byte[0]);
        when(mock.getTopic()).thenReturn(TOPIC_NAME);
        final BlockingQueue<ReceivedMqttMessage> mqttQueue = getMqttQueue(processor);
        mqttQueue.add(mock);

        final ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        assertThrows(InvocationTargetException.class, () -> transferQueue(processor,
            (ProcessSession) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{ProcessSession.class}, (proxy, method, args) -> {
                if (method.getName().equals("commitAsync")) {
                    throw new RuntimeException();
                } else {
                    return method.invoke(session, args);
                }
            })));
        assertTrue(mqttQueue.contains(mock));
    }

    @Test
    void addTopicAttributesWithMultipleTopicSegments() {
        final String topic = "home/livingroom/temperature";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(4, attributes.size(), "Expected 4 attributes (1 for full topic + 3 for segments)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("home", attributes.get("mqtt.topic.segment.0"));
        assertEquals("livingroom", attributes.get("mqtt.topic.segment.1"));
        assertEquals("temperature", attributes.get("mqtt.topic.segment.2"));
        assertNull(attributes.get("mqtt.topic.segment.3"), "No further segments expected");
    }

    @Test
    void addTopicAttributesWithLeadingSlashInTopic() {
        final String topic = "/sensors/light";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(4, attributes.size(), "Expected 4 attributes (1 for full topic + 3 for segments)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("", attributes.get("mqtt.topic.segment.0"), "Segment 0 should be empty for leading slash");
        assertEquals("sensors", attributes.get("mqtt.topic.segment.1"));
        assertEquals("light", attributes.get("mqtt.topic.segment.2"));
        assertNull(attributes.get("mqtt.topic.segment.3"), "No further segments expected");
    }

    @Test
    void addTopicAttributesWithTrailingSlashInTopicName() {
        final String topic = "data/device/";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(4, attributes.size(), "Expected 4 attributes (1 for full topic + 3 for segments)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("data", attributes.get("mqtt.topic.segment.0"));
        assertEquals("device", attributes.get("mqtt.topic.segment.1"));
        assertEquals("", attributes.get("mqtt.topic.segment.2"), "Segment 2 should be empty for trailing slash");
        assertNull(attributes.get("mqtt.topic.segment.3"), "No further segments expected");
    }

    @Test
    void addTopicAttributesWithSingleSlashAsTopic() {
        final String topic = "/";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(3, attributes.size(), "Expected 3 attributes (1 for full topic + 2 for segments)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("", attributes.get("mqtt.topic.segment.0"));
        assertEquals("", attributes.get("mqtt.topic.segment.1"));
        assertNull(attributes.get("mqtt.topic.segment.2"), "No further segments expected");
    }

    @Test
    void addTopicAttributes_consecutiveSlashesTopic_addsCorrectly() {
        final String topic = "status//alerts";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(4, attributes.size(), "Expected 4 attributes (1 for full topic + 3 for segments)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("status", attributes.get("mqtt.topic.segment.0"));
        assertEquals("", attributes.get("mqtt.topic.segment.1"));
        assertEquals("alerts", attributes.get("mqtt.topic.segment.2"));
        assertNull(attributes.get("mqtt.topic.segment.3"), "No further segments expected");
    }

    @Test
    void addTopicAttributesWithEmptyTopic() {
        final String topic = "";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(1, attributes.size(), "Expected only 1 attribute (for full topic)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present and empty");
        assertNull(attributes.get("mqtt.topic.segment.0"), "No segments should be added for empty topic");
    }

    @Test
    void addTopicAttributesWithNullTopic() {
        final String topic = null;

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(1, attributes.size(), "Expected only 1 attribute (for full topic)");
        assertNull(attributes.get("mqtt.topic"), "Full topic should be null");
        assertNull(attributes.get("mqtt.topic.segment.0"), "No segments should be added for null topic");
    }

    @Test
    void addTopicAttributesWithTopicWithoutSlashes() {
        final String topic = "sensors";

        final Map<String, String> attributes = new HashMap<>();
        new ConsumeMQTT().addTopicAttributes(attributes, topic);

        assertEquals(2, attributes.size(), "Expected 2 attributes (1 for full topic + 1 for segment)");
        assertEquals(topic, attributes.get("mqtt.topic"), "Full topic should be present");
        assertEquals("sensors", attributes.get("mqtt.topic.segment.0"));
        assertNull(attributes.get("mqtt.topic.segment.1"), "No further segments expected");
    }

    private TestRunner initializeTestRunner() {
        if (mqttTestClient != null) {
            throw new IllegalStateException("mqttTestClient should be null, using ConsumeMQTT's default client!");
        }

        final TestRunner testRunner = TestRunners.newTestRunner(ConsumeMQTT.class);

        setCommonProperties(testRunner);

        return testRunner;
    }

    private TestRunner initializeTestRunner(MqttTestClient mqttTestClient) {
        final TestRunner testRunner = TestRunners.newTestRunner(new ConsumeMQTT() {
            @Override
            protected MqttClient createMqttClient() {
                return mqttTestClient;
            }
        });

        setCommonProperties(testRunner);

        return testRunner;
    }

    private void setCommonProperties(TestRunner testRunner) {
        testRunner.setProperty(ConsumeMQTT.PROP_BROKER_URI, BROKER_URI);
        testRunner.setProperty(ConsumeMQTT.PROP_CLIENTID, CLIENT_ID);
        testRunner.setProperty(ConsumeMQTT.PROP_TOPIC_FILTER, TOPIC_NAME);
        testRunner.setProperty(ConsumeMQTT.PROP_MAX_QUEUE_SIZE, INTERNAL_QUEUE_SIZE);
    }

    private static boolean isConnected(AbstractMQTTProcessor processor) throws NoSuchFieldException, IllegalAccessException {
        final Field f = AbstractMQTTProcessor.class.getDeclaredField("mqttClient");
        f.setAccessible(true);
        final MqttClient mqttClient = (MqttClient) f.get(processor);
        return mqttClient.isConnected();
    }


    public static void reconnect(ConsumeMQTT processor, ProcessContext context) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Method method = ConsumeMQTT.class.getDeclaredMethod("initializeClient", ProcessContext.class);
        method.setAccessible(true);
        method.invoke(processor, context);
    }

    @SuppressWarnings("unchecked")
    public static BlockingQueue<ReceivedMqttMessage> getMqttQueue(ConsumeMQTT consumeMQTT) throws IllegalAccessException, NoSuchFieldException {
        final Field mqttQueueField = ConsumeMQTT.class.getDeclaredField("mqttQueue");
        mqttQueueField.setAccessible(true);
        return (BlockingQueue<ReceivedMqttMessage>) mqttQueueField.get(consumeMQTT);
    }

    public static void transferQueue(ConsumeMQTT consumeMQTT, ProcessSession session) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method transferQueue = ConsumeMQTT.class.getDeclaredMethod("transferQueue", ProcessSession.class);
        transferQueue.setAccessible(true);
        transferQueue.invoke(consumeMQTT, session);
    }

    private void assertProvenanceEvents(int count) {
        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertNotNull(provenanceEvents);
        assertEquals(count, provenanceEvents.size());
        if (count > 0) {
            assertEquals(ProvenanceEventType.RECEIVE, provenanceEvents.getFirst().getEventType());
        }
    }

    private void publishMessage(final String payload, final int qos) {
        final StandardMqttMessage message = new StandardMqttMessage(payload.getBytes(StandardCharsets.UTF_8), qos, false);
        mqttTestClient.publish(TOPIC_NAME, message);
    }

    private static String addSSLContextService(TestRunner testRunner) throws InitializationException {
        final SSLContextService sslContextService = mock(SSLContextService.class);
        final String identifier = SSLContextService.class.getSimpleName();
        when(sslContextService.getIdentifier()).thenReturn(identifier);

        testRunner.addControllerService(identifier, sslContextService);
        testRunner.enableControllerService(sslContextService);
        return identifier;
    }
}

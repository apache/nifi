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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.mqtt.IoTDeviceMQTT;
import org.apache.nifi.processors.mqtt.common.MqttTestClient.ConnectType;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
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

import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.IoTDeviceMQTT.REL_SUCCESS;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_FALSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class TestIoTDeviceMqttCommon {

    public int PUBLISH_WAIT_MS = 1000;

    public Server MQTT_server;
    public TestRunner testRunner;
    public String broker;
    public String topic;

    public abstract void internalPublish(PublishMessage publishMessage, ConnectType type);
    public abstract void verifyPublishedMessage(byte[] payload, int qos, boolean retain);

    @Test
    public void testConsumeLastWillConfig() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_LAST_WILL_MESSAGE, "lastWill message");
        testRunner.assertNotValid();
        testRunner.setProperty(IoTDeviceMQTT.PROP_LAST_WILL_TOPIC, "lastWill topic");
        testRunner.assertNotValid();
        testRunner.setProperty(IoTDeviceMQTT.PROP_LAST_WILL_QOS, "1");
        testRunner.assertNotValid();
        testRunner.setProperty(IoTDeviceMQTT.PROP_LAST_WILL_RETAIN, "false");
        testRunner.assertValid();
    }

    @Test
    public void testConsumeQoS2() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage, ConnectType.Subscriber);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(IoTDeviceMQTT.REL_RECEIVED, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testConsumeQoS2NotCleanSession() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.setProperty(IoTDeviceMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);
        testRunner.assertValid();

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        iotDeviceMqtt.onUnscheduled(testRunner.getProcessContext());

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage, ConnectType.Subscriber);

        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(IoTDeviceMQTT.REL_RECEIVED, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }


    @Test
    public void testConsumeQoS1() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "1");
        testRunner.assertValid();

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage, ConnectType.Subscriber);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
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
    public void testConsumeQoS1NotCleanSession() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "1");
        testRunner.setProperty(IoTDeviceMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);
        testRunner.assertValid();

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        iotDeviceMqtt.onUnscheduled(testRunner.getProcessContext());

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage, ConnectType.Subscriber);

        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        testRunner.run(1, false, false);

        testRunner.assertTransferCount(IoTDeviceMQTT.REL_RECEIVED, 1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
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
    public void testConsumeQoS0() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "0");
        testRunner.assertValid();

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
        testMessage.setRetainFlag(false);

        internalPublish(testMessage, ConnectType.Subscriber);

        Thread.sleep(PUBLISH_WAIT_MS);

        testRunner.run(1, false, false);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
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
    public void testConsumeOnStoppedFinish() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        MqttMessage innerMessage = new MqttMessage();
        innerMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()).array());
        innerMessage.setQos(2);
        MQTTQueueMessage testMessage = new MQTTQueueMessage("testTopic", innerMessage);

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        iotDeviceMqtt.processSessionFactory = testRunner.getProcessSessionFactory();

        Field f = IoTDeviceMQTT.class.getDeclaredField("mqttQueue");
        f.setAccessible(true);
        LinkedBlockingQueue<MQTTQueueMessage> queue = (LinkedBlockingQueue<MQTTQueueMessage>) f.get(iotDeviceMqtt);
        queue.add(testMessage);

        iotDeviceMqtt.onUnscheduled(testRunner.getProcessContext());
        iotDeviceMqtt.onStopped(testRunner.getProcessContext());

        testRunner.assertTransferCount(IoTDeviceMQTT.REL_RECEIVED, 1);
        assertProvenanceEvents(1);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testConsumeResizeBuffer() throws Exception {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.setProperty(IoTDeviceMQTT.PROP_MAX_QUEUE_SIZE, "2");
        testRunner.assertValid();

        PublishMessage testMessage = new PublishMessage();
        testMessage.setPayload(ByteBuffer.wrap("testMessage".getBytes()));
        testMessage.setTopicName("testTopic");
        testMessage.setDupFlag(false);
        testMessage.setQos(AbstractMessage.QOSType.EXACTLY_ONCE);
        testMessage.setRetainFlag(false);

        IoTDeviceMQTT iotDeviceMqtt = (IoTDeviceMQTT) testRunner.getProcessor();
        iotDeviceMqtt.onScheduled(testRunner.getProcessContext());
        reconnect(iotDeviceMqtt, testRunner.getProcessContext());

        Thread.sleep(PUBLISH_WAIT_MS);

        assertTrue(isConnected(iotDeviceMqtt));

        internalPublish(testMessage, ConnectType.Subscriber);
        internalPublish(testMessage, ConnectType.Subscriber);

        Thread.sleep(PUBLISH_WAIT_MS);
        iotDeviceMqtt.onUnscheduled(testRunner.getProcessContext());

        testRunner.setProperty(IoTDeviceMQTT.PROP_MAX_QUEUE_SIZE, "1");
        testRunner.assertNotValid();

        testRunner.setProperty(IoTDeviceMQTT.PROP_MAX_QUEUE_SIZE, "3");
        testRunner.assertValid();

        testRunner.run(1);

        testRunner.assertTransferCount(IoTDeviceMQTT.REL_RECEIVED, 2);
        assertProvenanceEvents(2);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(IoTDeviceMQTT.REL_RECEIVED);
        MockFlowFile flowFile = flowFiles.get(0);

        flowFile.assertContentEquals("testMessage");
        flowFile.assertAttributeEquals(BROKER_ATTRIBUTE_KEY, broker);
        flowFile.assertAttributeEquals(TOPIC_ATTRIBUTE_KEY, "testTopic");
        flowFile.assertAttributeEquals(QOS_ATTRIBUTE_KEY, "2");
        flowFile.assertAttributeEquals(IS_DUPLICATE_ATTRIBUTE_KEY, "false");
        flowFile.assertAttributeEquals(IS_RETAINED_ATTRIBUTE_KEY, "false");
    }

    @Test
    public void testPublishQoS0() {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "0");

        testRunner.assertValid();

        String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertProvenanceEvents();

        verifyPublishedMessage(testMessage.getBytes(), 0, false);
    }

    @Test
    public void testPublishQoS1() {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "1");

        testRunner.assertValid();

        String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvents();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 1, false);
    }

    @Test
    public void testPublishQoS2NotCleanSession() {
        // Publisher executes synchronously so the only time whether its Clean or Not matters is when the processor stops in the middle of the publishing
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.setProperty(IoTDeviceMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvents();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, false);
    }

    @Test
    public void testPublishQoS2() {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvents();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, false);
    }

    @Test
    public void testPublishRetainQoS2() {
        testRunner.setProperty(IoTDeviceMQTT.PROP_QOS, "2");
        testRunner.setProperty(IoTDeviceMQTT.PROP_RETAIN, "true");

        testRunner.assertValid();

        String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvents();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, true);
    }

    private void assertProvenanceEvents(){
        List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertNotNull(provenanceEvents);
        assertEquals(1, provenanceEvents.size());
        assertEquals(ProvenanceEventType.SEND, provenanceEvents.get(0).getEventType());
    }

    private static boolean isConnected(AbstractMQTTProcessor processor) throws NoSuchFieldException, IllegalAccessException {
        Field f = AbstractMQTTProcessor.class.getDeclaredField("mqttClient");
        f.setAccessible(true);
        IMqttClient mqttClient = (IMqttClient) f.get(processor);
        return mqttClient.isConnected();
    }

    public static void reconnect(IoTDeviceMQTT processor, ProcessContext context) throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Method method = IoTDeviceMQTT.class.getDeclaredMethod("initializeClient", ProcessContext.class);
        method.setAccessible(true);
        method.invoke(processor, context);
    }

    public static BlockingQueue<MQTTQueueMessage> getMqttQueue(IoTDeviceMQTT iotDeviceMqtt) throws IllegalAccessException, NoSuchFieldException {
        Field mqttQueueField = IoTDeviceMQTT.class.getDeclaredField("mqttQueue");
        mqttQueueField.setAccessible(true);
        return (BlockingQueue<MQTTQueueMessage>) mqttQueueField.get(iotDeviceMqtt);
    }

    public static void transferQueue(IoTDeviceMQTT iotDeviceMqtt, ProcessSession session) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method transferQueue = IoTDeviceMQTT.class.getDeclaredMethod("transferQueue", ProcessSession.class);
        transferQueue.setAccessible(true);
        transferQueue.invoke(iotDeviceMqtt, session);
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

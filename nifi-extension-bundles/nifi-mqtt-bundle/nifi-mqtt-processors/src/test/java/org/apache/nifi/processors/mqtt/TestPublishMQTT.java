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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttTestClient;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessDemarcatedContentStrategy.PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_FAILURE;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessDemarcatedContentStrategy.PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_RECOVER;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessDemarcatedContentStrategy.PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_SUCCESS;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessRecordSetStrategy.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessRecordSetStrategy.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER;
import static org.apache.nifi.processors.mqtt.PublishMQTT.ProcessRecordSetStrategy.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS;
import static org.apache.nifi.processors.mqtt.PublishMQTT.REL_FAILURE;
import static org.apache.nifi.processors.mqtt.PublishMQTT.REL_SUCCESS;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_CLEAN_SESSION_FALSE;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtil.createJsonRecordSetReaderService;
import static org.apache.nifi.processors.mqtt.common.MqttTestUtil.createJsonRecordSetWriterService;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class TestPublishMQTT {

    private static final String BROKER_URI = "tcp://localhost:1883";
    private static final String TOPIC = "testTopic";
    private static final String RETAIN = "false";

    private MqttTestClient mqttTestClient;
    private TestRunner testRunner;

    @AfterEach
    public void cleanup() {
        testRunner = null;
        mqttTestClient = null;
    }

    @Test
    public void testRecordAndDemarcatorConfigurationTogetherIsInvalid() throws InitializationException {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(PublishMQTT.MESSAGE_DEMARCATOR, "\n");

        testRunner.assertNotValid();
    }

    @Test
    public void testQoS0() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.PROP_QOS, "0");

        testRunner.assertValid();

        final String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertProvenanceEvent();

        verifyPublishedMessage(testMessage.getBytes(), 0, false);
    }

    @Test
    public void testQoS1() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.PROP_QOS, "1");

        testRunner.assertValid();

        final String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 1, false);
    }

    @Test
    public void testQoS2NotCleanSession() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        // Publisher executes synchronously so the only time whether its Clean or Not matters is when the processor stops in the middle of the publishing
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.setProperty(PublishMQTT.PROP_CLEAN_SESSION, ALLOWABLE_VALUE_CLEAN_SESSION_FALSE);

        testRunner.assertValid();

        final String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, false);
    }

    @Test
    public void testQoS2() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");

        testRunner.assertValid();

        final String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, false);
    }

    @Test
    public void testRetainQoS2() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.setProperty(PublishMQTT.PROP_RETAIN, "true");

        testRunner.assertValid();

        final String testMessage = "testMessage";
        testRunner.enqueue(testMessage.getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testMessage.getBytes(), 2, true);
    }

    @Test
    public void testPublishRecords() throws InitializationException {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final ArrayNode testInput = createTestJsonInput();

        testRunner.enqueue(testInput.toString().getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS, 3));

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testInput.get(0).toString().getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(1).toString().getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(2).toString().getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.getFirst();
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        assertFalse(successfulFlowFile.getAttributes().containsKey(publishFailedIndexAttributeName), "Failed attribute should not be present on the FlowFile");
    }

    @Test
    public void testPublishRecordsFailed() throws InitializationException {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod()
                .doThrow(new RuntimeException("Second publish failed."))
                .when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final ArrayNode testInput = createTestJsonInput();

        testRunner.enqueue(testInput.toString().getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, 1));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.get(0).toString().getBytes(), 2, false);
        verifyNoMorePublished();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.getFirst();
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        assertEquals("1", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishRecordsAndFailAgainWhenPreviousPublishFailed() throws InitializationException {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod()
                .doThrow(new RuntimeException("Second publish failed."))
                .when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        final ArrayNode testInput = createTestJsonInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(testInput.toString().getBytes(), attributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, 2));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.get(1).toString().getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.getFirst();
        assertEquals("2", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishRecordsSuccessfullyWhenPreviousPublishFailed() throws InitializationException {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod().when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        testRunner.setProperty(PublishMQTT.RECORD_READER, createJsonRecordSetReaderService(testRunner));
        testRunner.setProperty(PublishMQTT.RECORD_WRITER, createJsonRecordSetWriterService(testRunner));
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        final ArrayNode testInput = createTestJsonInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(testInput.toString().getBytes(), attributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER, 3));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.get(1).toString().getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(2).toString().getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.getFirst();
        assertNull(successfulFlowFile.getAttribute(publishFailedIndexAttributeName),
                publishFailedIndexAttributeName + " is expected to be removed after all remaining records have been published successfully.");
    }

    @Test
    public void testPublishDemarcatedContent() {
        mqttTestClient = new MqttTestClient(MqttTestClient.ConnectType.Publisher);
        testRunner = initializeTestRunner(mqttTestClient);

        final String demarcator = "\n";

        testRunner.setProperty(PublishMQTT.MESSAGE_DEMARCATOR, demarcator);
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final List<String> testInput = createMultipleInput();

        testRunner.enqueue(String.join(demarcator, testInput).getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_SUCCESS, 3));

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        verifyPublishedMessage(testInput.get(0).getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(1).getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(2).getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.getFirst();
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        assertFalse(successfulFlowFile.getAttributes().containsKey(publishFailedIndexAttributeName), "Failed attribute should not be present on the FlowFile");
    }

    @Test
    public void testPublishDemarcatedContentFailed() {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod()
                .doThrow(new RuntimeException("Second publish failed."))
                .when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        final String demarcator = "\n";

        testRunner.setProperty(PublishMQTT.MESSAGE_DEMARCATOR, demarcator);
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final List<String> testInput = createMultipleInput();

        testRunner.enqueue(String.join(demarcator, testInput).getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_FAILURE, 1));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.getFirst().getBytes(), 2, false);
        verifyNoMorePublished();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.getFirst();
        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        assertEquals("1", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishDemarcatedContentAndFailAgainWhenPreviousPublishFailed() {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod()
                .doThrow(new RuntimeException("Second publish failed."))
                .when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        final String demarcator = "\n";

        testRunner.setProperty(PublishMQTT.MESSAGE_DEMARCATOR, demarcator);
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        final List<String> testInput = createMultipleInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(String.join(demarcator, testInput).getBytes(), attributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_FAILURE, 2));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.get(1).getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals(1, flowFiles.size());

        final MockFlowFile failedFlowFile = flowFiles.getFirst();
        assertEquals("2", failedFlowFile.getAttribute(publishFailedIndexAttributeName), "Only one record is expected to be published successfully.");
    }

    @Test
    public void testContinuePublishDemarcatedContentSuccessfullyWhenPreviousPublishFailed() {
        mqttTestClient = Mockito.spy(new MqttTestClient(MqttTestClient.ConnectType.Publisher));
        Mockito.doCallRealMethod().when(mqttTestClient).publish(any(), any());
        testRunner = initializeTestRunner(mqttTestClient);

        final String demarcator = "\n";

        testRunner.setProperty(PublishMQTT.MESSAGE_DEMARCATOR, demarcator);
        testRunner.setProperty(PublishMQTT.PROP_QOS, "2");
        testRunner.assertValid();

        final String publishFailedIndexAttributeName = testRunner.getProcessor().getIdentifier() + ATTR_PUBLISH_FAILED_INDEX_SUFFIX;
        final List<String> testInput = createMultipleInput();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(publishFailedIndexAttributeName, "1");
        testRunner.enqueue(String.join(demarcator, testInput).getBytes(), attributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertProvenanceEvent(String.format(PROVENANCE_EVENT_DETAILS_ON_DEMARCATED_MESSAGE_RECOVER, 3));

        verify(mqttTestClient, Mockito.times(2)).publish(any(), any());
        verifyPublishedMessage(testInput.get(1).getBytes(), 2, false);
        verifyPublishedMessage(testInput.get(2).getBytes(), 2, false);
        verifyNoMorePublished();

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile successfulFlowFile = flowFiles.getFirst();
        assertNull(successfulFlowFile.getAttribute(publishFailedIndexAttributeName),
                publishFailedIndexAttributeName + " is expected to be removed after all remaining records have been published successfully.");
    }

    private void verifyPublishedMessage(byte[] payload, int qos, boolean retain) {
        final Pair<String, StandardMqttMessage> lastPublished = mqttTestClient.getLastPublished();
        final String lastPublishedTopic = lastPublished.getLeft();
        final StandardMqttMessage lastPublishedMessage = lastPublished.getRight();
        assertEquals(Arrays.toString(payload), Arrays.toString(lastPublishedMessage.getPayload()));
        assertEquals(qos, lastPublishedMessage.getQos());
        assertEquals(retain, lastPublishedMessage.isRetained());
        assertEquals(TOPIC, lastPublishedTopic);
    }

    private void verifyNoMorePublished() {
        assertNull(mqttTestClient.getLastPublished(), "TestClient's queue should be empty.");
    }

    private ProvenanceEventRecord assertProvenanceEvent() {
        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertNotNull(provenanceEvents);
        assertEquals(1, provenanceEvents.size());

        final ProvenanceEventRecord event = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.SEND, event.getEventType());

        return event;
    }

    private void assertProvenanceEvent(String expectedDetails) {
        final ProvenanceEventRecord event = assertProvenanceEvent();
        assertEquals(expectedDetails, event.getDetails());
    }

    private static ArrayNode createTestJsonInput() {
        final ObjectMapper mapper = new ObjectMapper();

        return mapper.createArrayNode().addAll(asList(
                mapper.createObjectNode()
                        .put("recordId", 1)
                        .put("firstAttribute", "foo")
                        .put("secondAttribute", false),
                mapper.createObjectNode()
                        .put("recordId", 2)
                        .put("firstAttribute", "bar")
                        .put("secondAttribute", true),
                mapper.createObjectNode()
                        .put("recordId", 3)
                        .put("firstAttribute", "foobar")
                        .put("secondAttribute", false)
        ));
    }

    private static List<String> createMultipleInput() {
        return Arrays.asList("message1", "message2", "message3");
    }

    private TestRunner initializeTestRunner(MqttTestClient mqttTestClient) {
        final TestRunner testRunner = TestRunners.newTestRunner(new PublishMQTT() {
            @Override
            protected MqttClient createMqttClient() {
                return mqttTestClient;
            }
        });

        testRunner.setProperty(PublishMQTT.PROP_BROKER_URI, BROKER_URI);
        testRunner.setProperty(PublishMQTT.PROP_RETAIN, RETAIN);
        testRunner.setProperty(PublishMQTT.PROP_TOPIC, TOPIC);

        return testRunner;
    }
}

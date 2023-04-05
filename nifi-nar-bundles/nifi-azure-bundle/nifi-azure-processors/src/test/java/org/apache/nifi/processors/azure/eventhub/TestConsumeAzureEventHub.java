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
package org.apache.nifi.processors.azure.eventhub;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestConsumeAzureEventHub {
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String POLICY_NAME = "POLICY";
    private static final String POLICY_KEY = "POLICY_KEY";
    private static final String STORAGE_ACCOUNT_NAME = "STORAGE";
    private static final String STORAGE_ACCOUNT_KEY = "STORAGE_KEY";
    private static final String STORAGE_TOKEN = "?TOKEN";
    private static final String SERVICE_BUS_ENDPOINT = ".endpoint";
    private static final String CONSUMER_GROUP = "CONSUMER";
    private static final String PARTITION_ID = "0";
    private static final String FIRST_CONTENT = "CONTENT-1";
    private static final String SECOND_CONTENT = "CONTENT-2";
    private static final String THIRD_CONTENT = "CONTENT-3";
    private static final String FOURTH_CONTENT = "CONTENT-4";
    private static final String APPLICATION_PROPERTY = "application";
    private static final String APPLICATION_ATTRIBUTE_NAME = String.format("eventhub.property.%s", APPLICATION_PROPERTY);

    private static final String EXPECTED_TRANSIT_URI = String.format("amqps://%s%s/%s/ConsumerGroups/%s/Partitions/%s",
            EVENT_HUB_NAMESPACE,
            SERVICE_BUS_ENDPOINT,
            EVENT_HUB_NAME,
            CONSUMER_GROUP,
            PARTITION_ID
    );

    @Mock
    EventProcessorClient eventProcessorClient;

    @Mock
    PartitionContext partitionContext;

    @Mock
    CheckpointStore checkpointStore;

    @Mock
    RecordSetWriterFactory writerFactory;

    @Mock
    RecordSetWriter writer;

    @Mock
    RecordReaderFactory readerFactory;

    @Mock
    RecordReader reader;

    private MockConsumeAzureEventHub processor;

    private TestRunner testRunner;

    @BeforeEach
    public void setupProcessor() {
        processor = new MockConsumeAzureEventHub();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorConfigValidityWithManagedIdentityFlag() throws InitializationException {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        final MockRecordParser reader = new MockRecordParser();
        final MockRecordWriter writer = new MockRecordWriter();
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.addControllerService("reader", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(ConsumeAzureEventHub.RECORD_WRITER, "writer");
        testRunner.setProperty(ConsumeAzureEventHub.RECORD_READER, "reader");
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.USE_MANAGED_IDENTITY,"true");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorConfigValidityWithNeitherStorageKeyNorTokenSet() {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.ACCESS_POLICY_NAME, POLICY_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.assertNotValid();
    }

    @Test
    public void testProcessorConfigValidityWithBothStorageKeyAndTokenSet() {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.ACCESS_POLICY_NAME, POLICY_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_KEY);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_SAS_TOKEN, STORAGE_TOKEN);
        testRunner.assertNotValid();
    }

    @Test
    public void testProcessorConfigValidityWithTokenSet() {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.ACCESS_POLICY_NAME, POLICY_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_SAS_TOKEN, STORAGE_TOKEN);
        testRunner.assertValid();
        testRunner.setProperty(ConsumeAzureEventHub.TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS.getValue());
        testRunner.assertValid();
    }

    @Test
    public void testProcessorConfigValidityWithStorageKeySet() {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.ACCESS_POLICY_NAME, POLICY_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_KEY);
        testRunner.assertValid();
    }

    @Test
    public void testReceiveOne() {
        setProperties();
        testRunner.run(1, false);
        final List<EventData> events = getEvents(FIRST_CONTENT);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertContentEquals(FIRST_CONTENT);
        assertEventHubAttributesFound(flowFile);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveTwo() {
        setProperties();
        testRunner.run(1, false);
        final List<EventData> events = getEvents(FIRST_CONTENT, SECOND_CONTENT);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(2, flowFiles.size());
        final MockFlowFile msg1 = flowFiles.get(0);
        msg1.assertContentEquals(FIRST_CONTENT);
        final MockFlowFile msg2 = flowFiles.get(1);
        msg2.assertContentEquals(SECOND_CONTENT);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
    }

    @Test
    public void testReceiveRecords() throws Exception {
        setProperties();

        final List<EventData> events = getEvents(FIRST_CONTENT, SECOND_CONTENT);
        setupRecordReader(events);
        setupRecordWriter();

        testRunner.run(1, false);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals(FIRST_CONTENT + SECOND_CONTENT);
        assertEventHubAttributesFound(ff1);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveRecordReaderFailure() throws Exception {
        setProperties();

        final List<EventData> events = getEvents(FIRST_CONTENT, SECOND_CONTENT, THIRD_CONTENT, FOURTH_CONTENT);
        setupRecordReader(events, 2, null);
        setupRecordWriter();

        testRunner.run(1, false);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals(FIRST_CONTENT + SECOND_CONTENT + FOURTH_CONTENT);
        assertEventHubAttributesFound(ff1);

        final List<MockFlowFile> failedFFs = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals(THIRD_CONTENT);
        assertEventHubAttributesFound(failed1);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent1.getTransitUri());

        final ProvenanceEventRecord provenanceEvent2 = provenanceEvents.get(1);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent2.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent2.getTransitUri());
    }

    @Test
    public void testReceiveAllRecordFailure() throws Exception {
        setProperties();

        final List<EventData> events = getEvents(FIRST_CONTENT);
        setupRecordReader(events, 0, null);
        setRecordWriterProperty();

        testRunner.run(1, false);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        final List<MockFlowFile> failedFFs = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals(FIRST_CONTENT);
        assertEventHubAttributesFound(failed1);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveRecordWriterFailure() throws Exception {
        setProperties();

        final List<EventData> events = getEvents(FIRST_CONTENT, SECOND_CONTENT, THIRD_CONTENT, FOURTH_CONTENT);
        setupRecordReader(events, -1, SECOND_CONTENT);
        setupRecordWriter(SECOND_CONTENT);

        testRunner.run(1, false);

        final EventBatchContext eventBatchContext = new EventBatchContext(partitionContext, events, checkpointStore, null);
        processor.eventBatchProcessor.accept(eventBatchContext);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals(FIRST_CONTENT + THIRD_CONTENT + FOURTH_CONTENT);
        assertEventHubAttributesFound(ff1);

        final List<MockFlowFile> failedFFs = testRunner.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals(SECOND_CONTENT);
        assertEventHubAttributesFound(failed1);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent1.getTransitUri());

        final ProvenanceEventRecord provenanceEvent2 = provenanceEvents.get(1);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent2.getEventType());
        assertEquals(EXPECTED_TRANSIT_URI, provenanceEvent2.getTransitUri());
    }

    private void setProperties() {
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(ConsumeAzureEventHub.ACCESS_POLICY_NAME, POLICY_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_SAS_TOKEN, STORAGE_TOKEN);

        when(partitionContext.getEventHubName()).thenReturn(EVENT_HUB_NAME);
        when(partitionContext.getConsumerGroup()).thenReturn(CONSUMER_GROUP);
        when(partitionContext.getPartitionId()).thenReturn(PARTITION_ID);

        when(checkpointStore.updateCheckpoint(any(Checkpoint.class))).thenReturn(Mono.empty());
    }

    private Record toRecord(String value) {
        Map<String, Object> map = new HashMap<>();
        map.put("value", value);
        return new MapRecord(new SimpleRecordSchema(Collections.singletonList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))), map);
    }

    private void setupRecordWriter() throws Exception {
        setupRecordWriter(null);
    }

    private RecordSetWriterFactory setRecordWriterProperty() throws InitializationException {
        when(writerFactory.getIdentifier()).thenReturn(RecordSetWriterFactory.class.getName());

        testRunner.addControllerService(RecordSetWriterFactory.class.getName(), writerFactory);
        testRunner.enableControllerService(writerFactory);
        testRunner.setProperty(ConsumeAzureEventHub.RECORD_WRITER, RecordSetWriterFactory.class.getName());

        return writerFactory;
    }

    private void setupRecordWriter(String throwErrorWith) throws Exception {
        final RecordSetWriterFactory writerFactory = setRecordWriterProperty();
        final AtomicReference<OutputStream> outRef = new AtomicReference<>();
        when(writerFactory.createWriter(any(), any(), any(), any(FlowFile.class))).thenAnswer(invocation -> {
            outRef.set(invocation.getArgument(2));
            return writer;
        });
        when(writer.write(any(Record.class))).thenAnswer(invocation -> {
            final String value = (String) invocation.<Record>getArgument(0).getValue("value");
            if (throwErrorWith != null && throwErrorWith.equals(value)) {
                throw new IOException(MockConsumeAzureEventHub.class.getSimpleName());
            }
            outRef.get().write(value.getBytes(StandardCharsets.UTF_8));
            return WriteResult.of(1, Collections.emptyMap());
        });
    }

    private void setupRecordReader(List<EventData> eventDataList) throws Exception {
        setupRecordReader(eventDataList, -1, null);
    }

    private void setupRecordReader(List<EventData> eventDataList, int throwExceptionAt, String writeFailureWith) throws Exception {
        when(readerFactory.getIdentifier()).thenReturn(RecordReaderFactory.class.getName());

        testRunner.addControllerService(RecordReaderFactory.class.getName(), readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(ConsumeAzureEventHub.RECORD_READER, RecordReaderFactory.class.getName());

        when(readerFactory.createRecordReader(anyMap(), any(), anyLong(), any())).thenReturn(reader);
        final List<Record> recordList = eventDataList.stream()
                .map(eventData -> toRecord(eventData.getBodyAsString()))
                .collect(Collectors.toList());

        // Add null to indicate the end of records.
        final Function<List<Record>, List<Record>> addEndRecord = rs -> rs.stream()
                // If the record is simulated to throw an exception when writing, do not add a null record to avoid messing up indices.
                .flatMap(r -> r.getAsString("value").equals(writeFailureWith) ? Stream.of(r) : Stream.of(r, null))
                .collect(Collectors.toList());

        final List<Record> recordSetList = addEndRecord.apply(recordList);
        final Record[] records = recordSetList.toArray(new Record[0]);

        switch (throwExceptionAt) {
            case -1:
                when(reader.nextRecord())
                        .thenReturn(records[0], Arrays.copyOfRange(records, 1, records.length));
                break;
            case 0:
                when(reader.nextRecord())
                        .thenThrow(new MalformedRecordException(MockConsumeAzureEventHub.class.getSimpleName()))
                        .thenReturn(records[0], Arrays.copyOfRange(records, 1, records.length));
                break;
            default:
                final List<Record> recordList1 = addEndRecord.apply(recordList.subList(0, throwExceptionAt));
                final List<Record> recordList2 = addEndRecord.apply(recordList.subList(throwExceptionAt + 1, recordList.size()));
                final Record[] records1 = recordList1.toArray(new Record[0]);
                final Record[] records2 = recordList2.toArray(new Record[0]);
                when(reader.nextRecord())
                        .thenReturn(records1[0], Arrays.copyOfRange(records1, 1, records1.length))
                        .thenThrow(new MalformedRecordException(MockConsumeAzureEventHub.class.getSimpleName()))
                        .thenReturn(records2[0], Arrays.copyOfRange(records2, 1, records2.length));
        }
    }

    private void assertEventHubAttributesFound(final MockFlowFile flowFile) {
        flowFile.assertAttributeEquals("eventhub.name", EVENT_HUB_NAME);
        flowFile.assertAttributeEquals("eventhub.partition", PARTITION_ID);
        flowFile.assertAttributeEquals(APPLICATION_ATTRIBUTE_NAME, MockConsumeAzureEventHub.class.getSimpleName());
    }

    private List<EventData> getEvents(final String... contents) {
        return Arrays.stream(contents)
                .map(content -> {
                    final EventData eventData = new EventData(content);
                    eventData.getProperties().put(APPLICATION_PROPERTY, MockConsumeAzureEventHub.class.getSimpleName());
                    return eventData;
                })
                .collect(Collectors.toList());
    }

    private class MockConsumeAzureEventHub extends ConsumeAzureEventHub {

        @Override
        protected EventProcessorClient createClient(final ProcessContext context) {
            return eventProcessorClient;
        }

        @Override
        protected String getTransitUri(final PartitionContext partitionContext) {
            return EXPECTED_TRANSIT_URI;
        }
    }
}

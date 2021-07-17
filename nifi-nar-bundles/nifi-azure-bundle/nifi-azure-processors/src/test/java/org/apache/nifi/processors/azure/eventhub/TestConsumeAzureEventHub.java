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

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;

public class TestConsumeAzureEventHub {
    private static final String namespaceName = "nifi-azure-hub";
    private static final String eventHubName = "get-test";
    private static final String storageAccountName = "test-sa";
    private static final String storageAccountKey = "test-sa-key";

    private ConsumeAzureEventHub.EventProcessor eventProcessor;
    private MockProcessSession processSession;
    private SharedSessionState sharedState;
    private PartitionContext partitionContext;
    private ConsumeAzureEventHub processor;

    @Before
    public void setupProcessor() {
        processor = new ConsumeAzureEventHub();
        final ProcessorInitializationContext initContext = Mockito.mock(ProcessorInitializationContext.class);
        final String componentId = "componentId";
        when(initContext.getIdentifier()).thenReturn(componentId);
        MockComponentLog componentLog = new MockComponentLog(componentId, processor);
        when(initContext.getLogger()).thenReturn(componentLog);
        processor.initialize(initContext);

        final ProcessSessionFactory processSessionFactory = Mockito.mock(ProcessSessionFactory.class);
        processor.setProcessSessionFactory(processSessionFactory);
        processor.setNamespaceName("namespace");

        sharedState = new SharedSessionState(processor, new AtomicLong(0));
        processSession = new MockProcessSession(sharedState, processor);
        when(processSessionFactory.createSession()).thenReturn(processSession);

        eventProcessor = processor.new EventProcessor();

        partitionContext = Mockito.mock(PartitionContext.class);
        when(partitionContext.getEventHubPath()).thenReturn("eventhub-name");
        when(partitionContext.getPartitionId()).thenReturn("partition-id");
        when(partitionContext.getConsumerGroupName()).thenReturn("consumer-group");
    }

    @Test
    public void testProcessorConfigValidityWithManagedIdentityFlag() throws InitializationException {
        TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(ConsumeAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.assertNotValid();
        testRunner.setProperty(ConsumeAzureEventHub.NAMESPACE,namespaceName);
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
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_NAME, storageAccountName);
        testRunner.setProperty(ConsumeAzureEventHub.STORAGE_ACCOUNT_KEY, storageAccountKey);
        testRunner.assertNotValid();

        testRunner.setProperty(ConsumeAzureEventHub.USE_MANAGED_IDENTITY,"true");
        testRunner.assertValid();
    }

    @Test
    public void testReceivedApplicationProperties() throws Exception {
        final EventData singleEvent = EventData.create("one".getBytes(StandardCharsets.UTF_8));
        singleEvent.getProperties().put("event-sender", "Apache NiFi");
        singleEvent.getProperties().put("application", "TestApp");
        final Iterable<EventData> eventDataList = Arrays.asList(singleEvent);
        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile msg1 = flowFiles.get(0);
        msg1.assertAttributeEquals("eventhub.property.event-sender", "Apache NiFi");
        msg1.assertAttributeEquals("eventhub.property.application", "TestApp");
    }

    @Test
    public void testReceiveOne() throws Exception {
        final Iterable<EventData> eventDataList = Arrays.asList(EventData.create("one".getBytes(StandardCharsets.UTF_8)));
        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile msg1 = flowFiles.get(0);
        msg1.assertContentEquals("one");
        msg1.assertAttributeEquals("eventhub.name", "eventhub-name");
        msg1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveTwo() throws Exception {
        final Iterable<EventData> eventDataList = Arrays.asList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8)),
                EventData.create("two".getBytes(StandardCharsets.UTF_8))
        );
        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(2, flowFiles.size());
        final MockFlowFile msg1 = flowFiles.get(0);
        msg1.assertContentEquals("one");
        final MockFlowFile msg2 = flowFiles.get(1);
        msg2.assertContentEquals("two");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
    }

    @Test
    public void testCheckpointFailure() throws Exception {
        final Iterable<EventData> eventDataList = Arrays.asList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8)),
                EventData.create("two".getBytes(StandardCharsets.UTF_8))
        );
        doThrow(new RuntimeException("Failed to create a checkpoint.")).when(partitionContext).checkpoint();
        eventProcessor.onEvents(partitionContext, eventDataList);

        // Even if it fails to create a checkpoint, these FlowFiles are already committed.
        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(2, flowFiles.size());
        final MockFlowFile msg1 = flowFiles.get(0);
        msg1.assertContentEquals("one");
        final MockFlowFile msg2 = flowFiles.get(1);
        msg2.assertContentEquals("two");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
    }

    private Record toRecord(String value) {
        Map<String, Object> map = new HashMap<>();
        map.put("value", value);
        return new MapRecord(new SimpleRecordSchema(Collections.singletonList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))), map);
    }

    private void setupRecordWriter() throws SchemaNotFoundException, IOException {
        setupRecordWriter(null);
    }

    private void setupRecordWriter(String throwErrorWith) throws SchemaNotFoundException, IOException {
        final RecordSetWriterFactory writerFactory = mock(RecordSetWriterFactory.class);
        processor.setWriterFactory(writerFactory);
        final RecordSetWriter writer = mock(RecordSetWriter.class);
        final AtomicReference<OutputStream> outRef = new AtomicReference<>();
        when(writerFactory.createWriter(any(), any(), any(), any(FlowFile.class))).thenAnswer(invocation -> {
            outRef.set(invocation.getArgument(2));
            return writer;
        });
        when(writer.write(any(Record.class))).thenAnswer(invocation -> {
            final String value = (String) invocation.<Record>getArgument(0).getValue("value");
            if (throwErrorWith != null && throwErrorWith.equals(value)) {
                throw new IOException("Simulating record write failure.");
            }
            outRef.get().write(value.getBytes(StandardCharsets.UTF_8));
            return WriteResult.of(1, Collections.emptyMap());
        });
    }

    private void setupRecordReader(List<EventData> eventDataList) throws MalformedRecordException, IOException, SchemaNotFoundException {
        setupRecordReader(eventDataList, -1, null);
    }

    private void setupRecordReader(List<EventData> eventDataList, int throwExceptionAt, String writeFailureWith)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        processor.setReaderFactory(readerFactory);
        final RecordReader reader = mock(RecordReader.class);
        when(readerFactory.createRecordReader(anyMap(), any(), anyLong(), any())).thenReturn(reader);
        final List<Record> recordList = eventDataList.stream()
                .map(eventData -> toRecord(new String(eventData.getBytes())))
                .collect(Collectors.toList());

        // Add null to indicate the end of records.
        final Function<List<Record>, List<Record>> addEndRecord = rs -> rs.stream()
                // If the record is simulated to throw an exception when writing, do not add a null record to avoid messing up indices.
                .flatMap(r -> r.getAsString("value").equals(writeFailureWith) ? Stream.of(r) : Stream.of(r, null))
                .collect(Collectors.toList());

        final List<Record> recordSetList = addEndRecord.apply(recordList);
        final Record[] records = recordSetList.toArray(new Record[recordSetList.size()]);

        switch (throwExceptionAt) {
            case -1:
                when(reader.nextRecord())
                        .thenReturn(records[0], Arrays.copyOfRange(records, 1, records.length));
                break;
            case 0:
                when(reader.nextRecord())
                        .thenThrow(new MalformedRecordException("Simulating Record parse failure."))
                        .thenReturn(records[0], Arrays.copyOfRange(records, 1, records.length));
                break;
            default:
                final List<Record> recordList1 = addEndRecord.apply(recordList.subList(0, throwExceptionAt));
                final List<Record> recordList2 = addEndRecord.apply(recordList.subList(throwExceptionAt + 1, recordList.size()));
                final Record[] records1 = recordList1.toArray(new Record[recordList1.size()]);
                final Record[] records2 = recordList2.toArray(new Record[recordList2.size()]);
                when(reader.nextRecord())
                        .thenReturn(records1[0], Arrays.copyOfRange(records1, 1, records1.length))
                        .thenThrow(new MalformedRecordException("Simulating Record parse failure."))
                        .thenReturn(records2[0], Arrays.copyOfRange(records2, 1, records2.length));
        }
    }

    @Test
    public void testReceiveRecords() throws Exception {
        final List<EventData> eventDataList = Arrays.asList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8)),
                EventData.create("two".getBytes(StandardCharsets.UTF_8))
        );

        setupRecordReader(eventDataList);

        setupRecordWriter();

        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals("onetwo");
        ff1.assertAttributeEquals("eventhub.name", "eventhub-name");
        ff1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveRecordReaderFailure() throws Exception {
        final List<EventData> eventDataList = Arrays.asList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8)),
                EventData.create("two".getBytes(StandardCharsets.UTF_8)),
                EventData.create("three".getBytes(StandardCharsets.UTF_8)),
                EventData.create("four".getBytes(StandardCharsets.UTF_8))
        );

        setupRecordReader(eventDataList, 2, null);

        setupRecordWriter();

        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals("onetwofour");
        ff1.assertAttributeEquals("eventhub.name", "eventhub-name");
        ff1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<MockFlowFile> failedFFs = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals("three");
        failed1.assertAttributeEquals("eventhub.name", "eventhub-name");
        failed1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent1.getTransitUri());

        final ProvenanceEventRecord provenanceEvent2 = provenanceEvents.get(1);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent2.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent2.getTransitUri());
    }

    @Test
    public void testReceiveAllRecordFailure() throws Exception {
        final List<EventData> eventDataList = Collections.singletonList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8))
        );

        setupRecordReader(eventDataList, 0, null);

        setupRecordWriter();

        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        final List<MockFlowFile> failedFFs = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals("one");
        failed1.assertAttributeEquals("eventhub.name", "eventhub-name");
        failed1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent1.getTransitUri());
    }

    @Test
    public void testReceiveRecordWriterFailure() throws Exception {
        final List<EventData> eventDataList = Arrays.asList(
                EventData.create("one".getBytes(StandardCharsets.UTF_8)),
                EventData.create("two".getBytes(StandardCharsets.UTF_8)),
                EventData.create("three".getBytes(StandardCharsets.UTF_8)),
                EventData.create("four".getBytes(StandardCharsets.UTF_8))
        );

        setupRecordReader(eventDataList, -1, "two");

        setupRecordWriter("two");

        eventProcessor.onEvents(partitionContext, eventDataList);

        processSession.assertCommitted();
        final List<MockFlowFile> flowFiles = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile ff1 = flowFiles.get(0);
        ff1.assertContentEquals("onethreefour");
        ff1.assertAttributeEquals("eventhub.name", "eventhub-name");
        ff1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<MockFlowFile> failedFFs = processSession.getFlowFilesForRelationship(ConsumeAzureEventHub.REL_PARSE_FAILURE);
        assertEquals(1, failedFFs.size());
        final MockFlowFile failed1 = failedFFs.get(0);
        failed1.assertContentEquals("two");
        failed1.assertAttributeEquals("eventhub.name", "eventhub-name");
        failed1.assertAttributeEquals("eventhub.partition", "partition-id");

        final List<ProvenanceEventRecord> provenanceEvents = sharedState.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());

        final ProvenanceEventRecord provenanceEvent1 = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent1.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent1.getTransitUri());

        final ProvenanceEventRecord provenanceEvent2 = provenanceEvents.get(1);
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent2.getEventType());
        assertEquals("amqps://namespace.servicebus.windows.net/" +
                "eventhub-name/ConsumerGroups/consumer-group/Partitions/partition-id", provenanceEvent2.getTransitUri());
    }
}

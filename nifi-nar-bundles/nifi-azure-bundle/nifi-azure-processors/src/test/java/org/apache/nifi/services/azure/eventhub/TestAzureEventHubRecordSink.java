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
package org.apache.nifi.services.azure.eventhub;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Map;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class TestAzureEventHubRecordSink {
    private static final String EVENT_HUB_NAMESPACE = "namespace";
    private static final String EVENT_HUB_NAME = "hub";
    private static final String POLICY_KEY = "policyKey";
    private static final String NULL_HEADER = null;
    private static final String WRITER_IDENTIFIER = MockRecordWriter.class.getSimpleName();
    private static final String IDENTIFIER = AzureEventHubRecordSink.class.getSimpleName();
    private static final String ID_FIELD = "id";
    private static final String ID_FIELD_VALUE = TestAzureEventHubRecordSink.class.getSimpleName();
    private static final RecordSchema RECORD_SCHEMA = getRecordSchema();
    private static final boolean SEND_ZERO_RESULTS = true;

    @Mock
    private EventHubProducerClient client;

    @Mock
    private EventDataBatch eventDataBatch;

    private AzureEventHubRecordSink azureEventHubRecordSink;

    @BeforeEach
    void setRunner() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.setValidateExpressionUsage(false);

        final MockRecordWriter recordWriter = new MockRecordWriter(NULL_HEADER, false);
        runner.addControllerService(WRITER_IDENTIFIER, recordWriter);
        runner.enableControllerService(recordWriter);

        azureEventHubRecordSink = new MockAzureEventHubRecordSink();
        runner.addControllerService(IDENTIFIER, azureEventHubRecordSink);
        runner.setProperty(azureEventHubRecordSink, AzureEventHubRecordSink.EVENT_HUB_NAME, EVENT_HUB_NAME);
        runner.setProperty(azureEventHubRecordSink, AzureEventHubRecordSink.EVENT_HUB_NAMESPACE, EVENT_HUB_NAMESPACE);
        runner.setProperty(azureEventHubRecordSink, AzureEventHubRecordSink.SHARED_ACCESS_POLICY_KEY, POLICY_KEY);
        runner.setProperty(azureEventHubRecordSink, AzureEventHubRecordSink.RECORD_WRITER_FACTORY, WRITER_IDENTIFIER);
        runner.setProperty(azureEventHubRecordSink, AzureEventHubRecordSink.TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS.getValue());
        runner.enableControllerService(azureEventHubRecordSink);
    }

    @Test
    void testSendDataNoRecords() throws IOException {
        when(client.createBatch(any(CreateBatchOptions.class))).thenReturn(eventDataBatch);

        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA);
        final WriteResult writeResult = azureEventHubRecordSink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS);

        assertNotNull(writeResult);
        assertEquals(0, writeResult.getRecordCount());

        verify(client, times(0)).send(any(EventDataBatch.class));
    }

    @Test
    void testSendDataOneRecordException() {
        when(client.createBatch(any(CreateBatchOptions.class))).thenReturn(eventDataBatch);
        when(eventDataBatch.tryAdd(isA(EventData.class))).thenReturn(false);

        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA, getRecords(1));
        assertThrows(ProcessException.class, ()-> azureEventHubRecordSink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS));

        verify(client, never()).send(any(EventDataBatch.class));
    }

    @Test
    void testSendDataOneRecord() throws IOException {
        when(client.createBatch(any(CreateBatchOptions.class))).thenReturn(eventDataBatch);
        when(eventDataBatch.tryAdd(isA(EventData.class))).thenReturn(true);

        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA, getRecords(1));
        final WriteResult writeResult = azureEventHubRecordSink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS);

        assertNotNull(writeResult);
        assertEquals(1, writeResult.getRecordCount());
    }

    @Test
    void testSendDataTwoRecords() throws IOException {
        when(client.createBatch(any(CreateBatchOptions.class))).thenReturn(eventDataBatch);
        when(eventDataBatch.tryAdd(isA(EventData.class))).thenReturn(true);

        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA, getRecords(2));
        final WriteResult writeResult = azureEventHubRecordSink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS);

        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
    }

    public class MockAzureEventHubRecordSink extends AzureEventHubRecordSink {
        @Override
        protected EventHubProducerClient createEventHubClient(
                final String namespace,
                final String serviceBusEndpoint,
                final String eventHubName,
                final String policyName,
                final String policyKey,
                final AzureAuthenticationStrategy authenticationStrategy,
                final AmqpTransportType transportType) throws ProcessException {
            return client;
        }
    }

    private static RecordSchema getRecordSchema() {
        final RecordField idField = new RecordField(ID_FIELD, RecordFieldType.STRING.getDataType());
        return new SimpleRecordSchema(Collections.singletonList(idField));
    }

    private static Record[] getRecords(int numberOfRecords) {
        final Map<String, Object> values = Collections.singletonMap(ID_FIELD, ID_FIELD_VALUE);
        final Record record = new MapRecord(RECORD_SCHEMA, values);
        final Record[] records = new Record[numberOfRecords];
        Arrays.fill(records, record);
        return records;
    }
}

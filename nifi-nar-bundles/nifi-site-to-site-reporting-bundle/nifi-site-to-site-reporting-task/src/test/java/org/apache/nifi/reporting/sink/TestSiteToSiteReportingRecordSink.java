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

package org.apache.nifi.reporting.sink;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestSiteToSiteReportingRecordSink {

    private ConfigurationContext context;

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteReportingRecordSink.RECORD_WRITER_FACTORY, "record-writer");
        MockSiteToSiteReportingRecordSink task = initTask(properties);

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("field1", 15);
        row1.put("field2", "Hello");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("field1", 6);
        row2.put("field2", "World!");

        RecordSet recordSet = new ListRecordSet(recordSchema, Arrays.asList(
                new MapRecord(recordSchema, row1),
                new MapRecord(recordSchema, row2)
        ));

        task.sendData(recordSet, new HashMap<>(), true);

        assertEquals(1, task.dataSent.size());
        String[] lines = new String(task.dataSent.get(0)).split("\n");
        assertNotNull(lines);
        assertEquals(2, lines.length);
        String[] data = new String(lines[0]).split(",");
        assertEquals("15", data[0]); // In the MockRecordWriter all values are strings
        assertEquals("Hello", data[1]);
        data = new String(lines[1]).split(",");
        assertEquals("6", data[0]);
        assertEquals("World!", data[1]);
    }

    @Test
    public void testNoRows() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteReportingRecordSink.RECORD_WRITER_FACTORY, "record-writer");
        MockSiteToSiteReportingRecordSink task = initTask(properties);

        List<RecordField> recordFields = Arrays.asList(
                new RecordField("field1", RecordFieldType.INT.getDataType()),
                new RecordField("field2", RecordFieldType.STRING.getDataType())
        );
        RecordSchema recordSchema = new SimpleRecordSchema(recordFields);

        task.sendData(RecordSet.of(recordSchema), new HashMap<>(), true);

        // One entry of an empty byte array
        assertEquals(1, task.dataSent.size());
        assertEquals(0, task.dataSent.get(0).length);

        task.sendData(RecordSet.of(recordSchema), new HashMap<>(), false);
        // Still only one entry even after two sends (toggled sendZeroResults)
        assertEquals(1, task.dataSent.size());
    }

    public MockSiteToSiteReportingRecordSink initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException, IOException {

        final MockSiteToSiteReportingRecordSink task = new MockSiteToSiteReportingRecordSink();
        context = Mockito.mock(ConfigurationContext.class);
        StateManager stateManager = new MockStateManager(task);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don't quote values
        Mockito.when(context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY)).thenReturn(pValue);
        Mockito.when(pValue.asControllerService(RecordSetWriterFactory.class)).thenReturn(writer);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ControllerServiceInitializationContext initContext = new MockControllerServiceInitializationContext(writer, UUID.randomUUID().toString(), logger, stateManager);
        task.initialize(initContext);

        return task;
    }

    private static final class MockSiteToSiteReportingRecordSink extends SiteToSiteReportingRecordSink {

        public MockSiteToSiteReportingRecordSink() throws IOException {
            super();
        }

        final List<byte[]> dataSent = new ArrayList<>();
        final MockRecordWriter writer = new MockRecordWriter(null, false); // No header, don't quote values

        @Override
        protected SiteToSiteClient getClient() {
            final SiteToSiteClient client = Mockito.mock(SiteToSiteClient.class);
            final Transaction transaction = Mockito.mock(Transaction.class);

            try {
                Mockito.doAnswer((Answer<Object>) invocation -> {
                    final byte[] data = invocation.getArgument(0, byte[].class);
                    dataSent.add(data);
                    return null;
                }).when(transaction).send(Mockito.any(byte[].class), Mockito.any(Map.class));

                Mockito.when(client.createTransaction(Mockito.any(TransferDirection.class))).thenReturn(transaction);
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail(e.toString());
            }

            return client;
        }

        @Override
        protected RecordSetWriterFactory getWriterFactory() {
            return writer;
        }
    }

}
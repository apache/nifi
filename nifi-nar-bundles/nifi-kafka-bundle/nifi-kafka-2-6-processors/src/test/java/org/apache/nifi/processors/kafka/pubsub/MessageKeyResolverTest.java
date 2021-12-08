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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class MessageKeyResolverTest {

    private PublisherPool mockPool;
    private PublisherLease mockLease;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException, IOException {
        mockPool = mock(PublisherPool.class);
        mockLease = mock(PublisherLease.class);
        Mockito.doCallRealMethod().when(mockLease).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
                any(RecordSchema.class), any(PropertyValue.class), any(String.class), nullable(Function.class), any(MessageKeyResolver.class));

        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        runner = TestRunners.newTestRunner(new PublishKafkaRecord_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return mockPool;
            }
        });
    }

    @Test
    public void testMESSAGE_VALUE_PROPERTY_RESOLVER() {
        final MockFlowFile flowFile = getTestFlowFile();
        final Record record = getTestRecord();

        assertEquals("48", MessageKeyResolvers.MESSAGE_VALUE_PROPERTY_RESOLVER.apply(flowFile, record, new MockPropertyValue("id")));
        assertNull(MessageKeyResolvers.MESSAGE_VALUE_PROPERTY_RESOLVER.apply(flowFile, record, new MockPropertyValue("XXX")));
    }

   @Test
    public void testRECORD_PATH_RESOLVER() {
        final MockFlowFile flowFile = getTestFlowFile();
        final Record record = getTestRecord();

       RecordPathCache cache = new RecordPathCache(25);
       final MessageKeyResolver resolver = (MessageKeyResolvers.RECORD_PATH_RESOLVER_FACTORY(cache));
        assertEquals("48", resolver.apply(flowFile, record, new MockPropertyValue("/id")));
        assertEquals("1", resolver.apply(flowFile, record, new MockPropertyValue("/mainAccount/id")));
        // This is a particularly interesting case: a formally valid, but non-existing path results in a match with StandardFieldValue(null) ...
        assertNull(resolver.apply(flowFile, record, new MockPropertyValue("/XXX")));
    }

   @Test
    public void testEXPRESSION_LANGUAGE_RESOLVER() {
        final MockFlowFile flowFile = getTestFlowFile();
        final Record record = getTestRecord();

        final MessageKeyResolver resolver = MessageKeyResolvers.SIMPLE_VALUE_RESOLVER;
        assertEquals("48", resolver.apply(flowFile, record, new MockPropertyValue("48")));
    }


    private MockFlowFile getTestFlowFile() {
        return runner.enqueue("John Doe, 48");
    }

    private Record getTestRecord() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        return new MapRecord(schema, values);
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        return new SimpleRecordSchema(accountFields);
    }

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("mainAccount", RecordFieldType.RECORD.getRecordDataType(getAccountSchema())));
        fields.add(new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));

        final DataType accountDataType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountDataType);
        final RecordField accountsField = new RecordField("accounts", accountsType);
        fields.add(accountsField);
        return fields;
    }

}

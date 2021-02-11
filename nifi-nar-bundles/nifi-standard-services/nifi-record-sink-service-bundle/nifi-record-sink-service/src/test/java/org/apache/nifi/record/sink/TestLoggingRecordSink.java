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
package org.apache.nifi.record.sink;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class TestLoggingRecordSink {

    private LoggingRecordSink recordSink;
    private RecordSet recordSet;

    @Before
    public void setup() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        recordSink = new LoggingRecordSink();
        runner.addControllerService("log", recordSink);

        MockRecordWriter writerFactory = new MockRecordWriter();
        runner.addControllerService("writer", writerFactory);
        runner.setProperty(recordSink, LoggingRecordSink.RECORD_WRITER_FACTORY, "writer");

        runner.enableControllerService(writerFactory);
        runner.enableControllerService(recordSink);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("a", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.BOOLEAN.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap1 = new HashMap<>();
        valueMap1.put("a", "Hello");
        valueMap1.put("b", true);
        final Record record1 = new MapRecord(schema, valueMap1);

        final Map<String, Object> valueMap2 = new HashMap<>();
        valueMap2.put("a", "World");
        valueMap2.put("b", false);
        final Record record2 = new MapRecord(schema, valueMap2);

        recordSet = RecordSet.of(schema, record1, record2);
    }

    @Test
    public void testLogging() {
        try {
            final WriteResult writeResult = recordSink.sendData(recordSet, Collections.emptyMap(), false);
            assertNotNull(writeResult);
            assertEquals(2, writeResult.getRecordCount());
        } catch (IOException ioe) {
            fail("Should have completed successfully");
        }
    }

}
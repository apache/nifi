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
package org.apache.nifi.grok;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestGrokReader {
    private TestRunner runner;

    private static final String TIMESTAMP_FIELD = "timestamp";

    private static final String LEVEL_FIELD = "level";

    private static final String FACILITY_FIELD = "facility";

    private static final String PROGRAM_FIELD = "program";

    private static final String MESSAGE_FIELD = "message";

    private static final String STACKTRACE_FIELD = "stackTrace";

    private static final String RAW_FIELD = "_raw";

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @Test
    void testComplexGrokExpression() throws Exception {
        String input = "1021-09-09 09:03:06 127.0.0.1 nifi[1000]: LogMessage" + System.lineSeparator()
            + "October 19 19:13:16 127.0.0.1 nifi[1000]: LogMessage2" + System.lineSeparator();

        String grokPatternFile = "src/test/resources/grok/grok_patterns.txt";
        String grokExpression = "%{LINE}";

        SimpleRecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField(TIMESTAMP_FIELD, RecordFieldType.STRING.getDataType()),
            new RecordField(FACILITY_FIELD, RecordFieldType.STRING.getDataType()),
            new RecordField("priority", RecordFieldType.STRING.getDataType()),
            new RecordField("logsource", RecordFieldType.STRING.getDataType()),
            new RecordField(PROGRAM_FIELD, RecordFieldType.STRING.getDataType()),
            new RecordField("pid", RecordFieldType.STRING.getDataType()),
            new RecordField(MESSAGE_FIELD, RecordFieldType.STRING.getDataType()),
            new RecordField(STACKTRACE_FIELD, RecordFieldType.STRING.getDataType()),
            new RecordField(RAW_FIELD, RecordFieldType.STRING.getDataType())
        ));

        final Map<String, Object> expectedFirstMapValues = new HashMap<>();
        expectedFirstMapValues.put(TIMESTAMP_FIELD, "1021-09-09 09:03:06");
        expectedFirstMapValues.put(FACILITY_FIELD, null);
        expectedFirstMapValues.put("priority", null);
        expectedFirstMapValues.put("logsource", "127.0.0.1");
        expectedFirstMapValues.put(PROGRAM_FIELD, "nifi");
        expectedFirstMapValues.put("pid", "1000");
        expectedFirstMapValues.put("message", " LogMessage");
        expectedFirstMapValues.put(STACKTRACE_FIELD, null);
        expectedFirstMapValues.put(RAW_FIELD, "1021-09-09 09:03:06 127.0.0.1 nifi[1000]: LogMessage");
        final Record expectedFirstRecord = new MapRecord(expectedSchema, expectedFirstMapValues);

        final Map<String, Object> expectedSecondMapValues = new HashMap<>();
        expectedSecondMapValues.put(TIMESTAMP_FIELD, "October 19 19:13:16");
        expectedSecondMapValues.put(FACILITY_FIELD, null);
        expectedSecondMapValues.put("priority", null);
        expectedSecondMapValues.put("logsource", "127.0.0.1");
        expectedSecondMapValues.put(PROGRAM_FIELD, "nifi");
        expectedSecondMapValues.put("pid", "1000");
        expectedSecondMapValues.put(MESSAGE_FIELD, " LogMessage2");
        expectedSecondMapValues.put(STACKTRACE_FIELD, null);
        expectedSecondMapValues.put(RAW_FIELD, "October 19 19:13:16 127.0.0.1 nifi[1000]: LogMessage2");
        final Record expectedSecondRecord = new MapRecord(expectedSchema, expectedSecondMapValues);

        final GrokReader grokReader = new GrokReader();
        runner.addControllerService(GrokReader.class.getSimpleName(), grokReader);
        runner.setProperty(grokReader, GrokReader.GROK_PATTERNS, grokPatternFile);
        runner.setProperty(grokReader, GrokReader.GROK_EXPRESSION, grokExpression);
        runner.enableControllerService(grokReader);

        final byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(inputBytes);
        final RecordReader recordReader = grokReader.createRecordReader(Collections.emptyMap(), inputStream, inputBytes.length, runner.getLogger());

        final Record firstRecord = recordReader.nextRecord();

        assertArrayEquals(expectedFirstRecord.getValues(), firstRecord.getValues());
        assertEquals(expectedSchema, firstRecord.getSchema());

        final Record secondRecord = recordReader.nextRecord();
        assertArrayEquals(expectedSecondRecord.getValues(), secondRecord.getValues());
        assertEquals(expectedSchema, secondRecord.getSchema());

        assertNull(recordReader.nextRecord());
    }

    @Test
    public void testMultipleExpressions() throws InitializationException, IOException, SchemaNotFoundException, MalformedRecordException {
        final String program = "NiFi";
        final String level = "INFO";
        final String message = "Processing Started";
        final String timestamp = "Jan 10 12:30:45";

        final String logs = String.format("%s %s %s%n%s %s %s %s%n", program, level, message, timestamp, program, level, message);
        final byte[] bytes = logs.getBytes(StandardCharsets.UTF_8);

        final String matchingExpression = "%{PROG:program} %{LOGLEVEL:level} %{GREEDYDATA:message}";
        final String firstExpression = "%{SYSLOGTIMESTAMP:timestamp} %{PROG:program} %{LOGLEVEL:level} %{GREEDYDATA:message}";
        final String expressions = String.format("%s%n%s", firstExpression, matchingExpression);

        final GrokReader grokReader = new GrokReader();
        runner.addControllerService(GrokReader.class.getSimpleName(), grokReader);
        runner.setProperty(grokReader, GrokReader.GROK_EXPRESSION, expressions);
        runner.setProperty(grokReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, GrokReader.STRING_FIELDS_FROM_GROK_EXPRESSION);
        runner.enableControllerService(grokReader);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        final RecordReader recordReader = grokReader.createRecordReader(Collections.emptyMap(), inputStream, bytes.length, runner.getLogger());

        final Record firstRecord = recordReader.nextRecord();
        assertNotNull(firstRecord);
        assertEquals(program, firstRecord.getValue(PROGRAM_FIELD));
        assertEquals(level, firstRecord.getValue(LEVEL_FIELD));
        assertEquals(message, firstRecord.getValue(MESSAGE_FIELD));
        assertNull(firstRecord.getValue(TIMESTAMP_FIELD));

        final Record secondRecord = recordReader.nextRecord();
        assertNotNull(secondRecord);
        assertEquals(program, secondRecord.getValue(PROGRAM_FIELD));
        assertEquals(level, secondRecord.getValue(LEVEL_FIELD));
        assertEquals(message, secondRecord.getValue(MESSAGE_FIELD));
        assertEquals(timestamp, secondRecord.getValue(TIMESTAMP_FIELD));

        assertNull(recordReader.nextRecord());
    }

    @Test
    public void testPatternsProperty() throws InitializationException, IOException, SchemaNotFoundException, MalformedRecordException {
        final String program = "NiFi";
        final String level = "INFO";
        final String message = "Processing Started";

        final String logs = String.format("%s %s %s%n", program, level, message);
        final byte[] bytes = logs.getBytes(StandardCharsets.UTF_8);

        final String matchingExpression = "%{PROGRAM:program} %{LOGLEVEL:level} %{GREEDYDATA:message}";

        final GrokReader grokReader = new GrokReader();
        runner.addControllerService(GrokReader.class.getSimpleName(), grokReader);
        runner.setProperty(grokReader, GrokReader.GROK_PATTERNS, "PROGRAM [a-zA-Z]+");
        runner.setProperty(grokReader, GrokReader.GROK_EXPRESSION, matchingExpression);
        runner.setProperty(grokReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, GrokReader.STRING_FIELDS_FROM_GROK_EXPRESSION);
        runner.enableControllerService(grokReader);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        final RecordReader recordReader = grokReader.createRecordReader(Collections.emptyMap(), inputStream, bytes.length, runner.getLogger());

        final Record firstRecord = recordReader.nextRecord();
        assertNotNull(firstRecord);
        assertEquals(program, firstRecord.getValue(PROGRAM_FIELD));
        assertEquals(level, firstRecord.getValue(LEVEL_FIELD));
        assertEquals(message, firstRecord.getValue(MESSAGE_FIELD));

        assertNull(recordReader.nextRecord());
    }
}

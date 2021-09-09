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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class TestGrokReader {
    private TestRunner runner;
    private List<Record> records;

    private static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
        .name("reader")
        .identifiesControllerService(GrokReader.class)
        .build();

    @BeforeEach
    void setUp() {
        Processor processor = new AbstractProcessor() {
            Relationship SUCCESS = new Relationship.Builder()
                .name("success")
                .build();

            @Override
            public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
                FlowFile flowFile = session.get();
                final RecordReaderFactory readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);

                try (final InputStream in = session.read(flowFile);
                     final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                    Record record;
                    while ((record = reader.nextRecord()) != null) {
                        records.add(record);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                session.transfer(flowFile, SUCCESS);
            }

            @Override
            protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return Arrays.asList(READER);
            }

            @Override
            public Set<Relationship> getRelationships() {
                return new HashSet<>(Arrays.asList(SUCCESS));
            }
        };

        runner = TestRunners.newTestRunner(processor);

        records = new ArrayList<>();
    }

    @Test
    void testComplexGrokExpression() throws Exception {
        // GIVEN
        String input = "1021-09-09 09:03:06 127.0.0.1 nifi[1000]: LogMessage" + System.lineSeparator()
            + "October 19 19:13:16 127.0.0.1 nifi[1000]: LogMessage2" + System.lineSeparator();

        String grokPatternFile = "src/test/resources/grok/grok_patterns.txt";
        String grokExpression = "%{LINE}";

        SimpleRecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("timestamp", RecordFieldType.STRING.getDataType()),
            new RecordField("facility", RecordFieldType.STRING.getDataType()),
            new RecordField("priority", RecordFieldType.STRING.getDataType()),
            new RecordField("logsource", RecordFieldType.STRING.getDataType()),
            new RecordField("program", RecordFieldType.STRING.getDataType()),
            new RecordField("pid", RecordFieldType.STRING.getDataType()),
            new RecordField("message", RecordFieldType.STRING.getDataType()),
            new RecordField("stackTrace", RecordFieldType.STRING.getDataType()),
            new RecordField("_raw", RecordFieldType.STRING.getDataType())
        ));

        List<Record> expectedRecords = Arrays.asList(
            new MapRecord(expectedSchema, new HashMap<String, Object>() {{
                put("timestamp", "1021-09-09 09:03:06");
                put("facility", null);
                put("priority", null);
                put("logsource", "127.0.0.1");
                put("program", "nifi");
                put("pid", "1000");
                put("message", " LogMessage");
                put("stackstrace", null);
                put("_raw", "1021-09-09 09:03:06 127.0.0.1 nifi[1000]: LogMessage");
            }}),
            new MapRecord(expectedSchema, new HashMap<String, Object>() {{
                put("timestamp", "October 19 19:13:16");
                put("facility", null);
                put("priority", null);
                put("logsource", "127.0.0.1");
                put("program", "nifi");
                put("pid", "1000");
                put("message", " LogMessage2");
                put("stackstrace", null);
                put("_raw", "October 19 19:13:16 127.0.0.1 nifi[1000]: LogMessage2");
            }})
        );

        // WHEN
        GrokReader grokReader = new GrokReader();

        runner.addControllerService("grokReader", grokReader);
        runner.setProperty(READER, "grokReader");

        runner.setProperty(grokReader, GrokReader.PATTERN_FILE, grokPatternFile);
        runner.setProperty(grokReader, GrokReader.GROK_EXPRESSION, grokExpression);

        runner.enableControllerService(grokReader);

        runner.enqueue(input);
        runner.run();

        // THEN
        List<Function<Record, Object>> propertyProviders = Arrays.asList(
            Record::getSchema,
            Record::getValues
        );

        List<EqualsWrapper<Record>> wrappedExpected = EqualsWrapper.wrapList(expectedRecords, propertyProviders);
        List<EqualsWrapper<Record>> wrappedActual = EqualsWrapper.wrapList(records, propertyProviders);

        Assertions.assertEquals(wrappedExpected, wrappedActual);
    }
}

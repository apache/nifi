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
package org.apache.nifi.kafka.processors.producer.convert;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.kafka.processors.PublishKafka;
import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.processors.producer.key.KeyFactory;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordMetadataStrategy;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class KafkaRecordConverterTest {

    private static final int EXPECTED_RECORD_COUNT = 50000; // Expect 50,000 records
    private static final int MAX_MESSAGE_SIZE = 5 * 1024 * 1024; // 5MB
    private static final byte[] LARGE_SAMPLE_INPUT = new byte[MAX_MESSAGE_SIZE];
    private static final Map<String, String> SAMPLE_ATTRIBUTES = Map.of("attribute1", "value1");

    @Mock
    private HeadersFactory headersFactory;

    @Mock
    private KeyFactory keyFactory;

    private JsonTreeReader jsonTreeReader;

    private JsonRecordSetWriter jsonRecordSetWriter;

    @BeforeEach
    void setUp() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new PublishKafka());

        MockitoAnnotations.openMocks(this);

        jsonTreeReader = new JsonTreeReader();

        runner.addControllerService("jsonTreeReader", jsonTreeReader);
        runner.setProperty(jsonTreeReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.enableControllerService(jsonTreeReader);

        jsonRecordSetWriter = new JsonRecordSetWriter();

        runner.addControllerService("jsonRecordSetWriter", jsonRecordSetWriter);
        runner.setProperty(jsonRecordSetWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonRecordSetWriter, "output-grouping", "output-oneline");
        runner.enableControllerService(jsonRecordSetWriter);

        populateSampleInput();
    }

    // Create 5MB of sample data with multiple records
    private static void populateSampleInput() {
        StringBuilder sb = new StringBuilder();
        int recordCount = EXPECTED_RECORD_COUNT;
        int approximateRecordSize = MAX_MESSAGE_SIZE / recordCount;

        for (int i = 0; i < recordCount; i++) {
            sb.append("{\"key\": \"").append(i).append("\",\"value\":\"");
            int payloadSize = approximateRecordSize - 30;
            for (int j = 0; j < payloadSize; j++) {
                sb.append((char) ('A' + (j % 26)));
            }
            sb.append("\"}");
            if (i < recordCount - 1) {
                sb.append("\n");
            }
            if (sb.length() >= MAX_MESSAGE_SIZE - 100) {
                break;
            }
        }

        byte[] stringBytes = sb.toString().getBytes();

        Arrays.fill(LARGE_SAMPLE_INPUT, (byte) ' ');  // Fill with spaces instead of NULL

        int copyLength = Math.min(stringBytes.length, LARGE_SAMPLE_INPUT.length);
        System.arraycopy(stringBytes, 0, LARGE_SAMPLE_INPUT, 0, copyLength);
    }

    private void verifyRecordCount(Iterator<KafkaRecord> records, int expectedCount) {
        List<KafkaRecord> recordList = new ArrayList<>();
        records.forEachRemaining(recordList::add);
        assertEquals(expectedCount, recordList.size(), "Expected exactly " + expectedCount + " records");
    }

    @Test
    void testDelimitedStreamKafkaRecordConverter() throws Exception {
        DelimitedStreamKafkaRecordConverter converter = new DelimitedStreamKafkaRecordConverter(new byte[]{'\n'}, MAX_MESSAGE_SIZE, headersFactory);
        InputStream inputStream = new ByteArrayInputStream(LARGE_SAMPLE_INPUT);
        verifyRecordCount(converter.convert(SAMPLE_ATTRIBUTES, inputStream, LARGE_SAMPLE_INPUT.length), EXPECTED_RECORD_COUNT);
    }

    @Test
    void testFlowFileStreamKafkaRecordConverter() throws Exception {
        FlowFileStreamKafkaRecordConverter converter = new FlowFileStreamKafkaRecordConverter(MAX_MESSAGE_SIZE, headersFactory, keyFactory);
        InputStream inputStream = new ByteArrayInputStream(LARGE_SAMPLE_INPUT);
        verifyRecordCount(converter.convert(SAMPLE_ATTRIBUTES, inputStream, LARGE_SAMPLE_INPUT.length), 1);
    }

    @Test
    void testRecordStreamKafkaRecordConverter() throws Exception {
        RecordStreamKafkaRecordConverter recordStreamKafkaRecordConverter = new RecordStreamKafkaRecordConverter(jsonTreeReader, jsonRecordSetWriter, headersFactory, keyFactory, Integer.MAX_VALUE,
                mock(ComponentLog.class));
        InputStream inputStream = new ByteArrayInputStream(LARGE_SAMPLE_INPUT);
        verifyRecordCount(recordStreamKafkaRecordConverter.convert(SAMPLE_ATTRIBUTES, inputStream, LARGE_SAMPLE_INPUT.length), EXPECTED_RECORD_COUNT);
    }

    @Test
    void testRecordWrapperStreamKafkaRecordConverter() throws Exception {
        RecordWrapperStreamKafkaRecordConverter recordWrapperStreamKafkaRecordConverter = new RecordWrapperStreamKafkaRecordConverter(new MockFlowFile(0), RecordMetadataStrategy.FROM_PROPERTIES,
                jsonTreeReader, jsonRecordSetWriter, jsonRecordSetWriter, Integer.MAX_VALUE, mock(ComponentLog.class));

        InputStream inputStream = new ByteArrayInputStream(LARGE_SAMPLE_INPUT);

        verifyRecordCount(recordWrapperStreamKafkaRecordConverter.convert(SAMPLE_ATTRIBUTES, inputStream, LARGE_SAMPLE_INPUT.length), EXPECTED_RECORD_COUNT);
    }
}
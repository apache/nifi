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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class KafkaRecordConverterTest {

    private static final int EXPECTED_RECORD_COUNT = 50000; // Expect 50,000 records
    private static final int MAX_MESSAGE_SIZE = 5 * 1024 * 1024; // 5MB
    private static final byte[] LARGE_SAMPLE_INPUT = new byte[MAX_MESSAGE_SIZE];
    private static final Map<String, String> SAMPLE_ATTRIBUTES = Map.of("attribute1", "value1");

    private static Stream<Arguments> converterProvider() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new PublishKafka());

        JsonTreeReader jsonTreeReader = new JsonTreeReader();

        runner.addControllerService("jsonTreeReader", jsonTreeReader);
        runner.setProperty(jsonTreeReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.enableControllerService(jsonTreeReader);

        JsonRecordSetWriter jsonRecordSetWriter = new JsonRecordSetWriter();

        runner.addControllerService("jsonRecordSetWriter", jsonRecordSetWriter);
        runner.setProperty(jsonRecordSetWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonRecordSetWriter, JsonRecordSetWriter.OUTPUT_GROUPING, "output-oneline");
        runner.enableControllerService(jsonRecordSetWriter);

        ComponentLog mockLog = mock(ComponentLog.class);
        HeadersFactory headersFactory = mock(HeadersFactory.class);
        KeyFactory keyFactory = mock(KeyFactory.class);

        DelimitedStreamKafkaRecordConverter delimitedStreamKafkaRecordConverter = new DelimitedStreamKafkaRecordConverter(new byte[] {'\n'}, MAX_MESSAGE_SIZE, headersFactory);
        FlowFileStreamKafkaRecordConverter flowFileStreamKafkaRecordConverter = new FlowFileStreamKafkaRecordConverter(MAX_MESSAGE_SIZE, headersFactory, keyFactory);
        RecordStreamKafkaRecordConverter recordStreamKafkaRecordConverter = new RecordStreamKafkaRecordConverter(jsonTreeReader, jsonRecordSetWriter, headersFactory, keyFactory, Integer.MAX_VALUE,
                mockLog);
        RecordWrapperStreamKafkaRecordConverter recordWrapperStreamKafkaRecordConverter = new RecordWrapperStreamKafkaRecordConverter(new MockFlowFile(0), RecordMetadataStrategy.FROM_PROPERTIES,
                jsonTreeReader, jsonRecordSetWriter, jsonRecordSetWriter, Integer.MAX_VALUE, mockLog);

        return Stream.of(
                Arguments.of(delimitedStreamKafkaRecordConverter, EXPECTED_RECORD_COUNT),
                Arguments.of(flowFileStreamKafkaRecordConverter, 1),
                Arguments.of(recordStreamKafkaRecordConverter, EXPECTED_RECORD_COUNT),
                Arguments.of(recordWrapperStreamKafkaRecordConverter, EXPECTED_RECORD_COUNT));
    }

    @ParameterizedTest
    @MethodSource("converterProvider")
    void testKafkaRecordConverter(KafkaRecordConverter converter, int expectedCount) throws Exception {
        populateSampleInput();

        InputStream inputStream = new ByteArrayInputStream(LARGE_SAMPLE_INPUT);
        List<KafkaRecord> recordList = new ArrayList<>();
        converter.convert(SAMPLE_ATTRIBUTES, inputStream, LARGE_SAMPLE_INPUT.length).forEachRemaining(recordList::add);

        assertEquals(expectedCount, recordList.size(), "Expected exactly " + expectedCount + " records");
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

}

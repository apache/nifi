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
package org.apache.nifi.processors.aws.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

final class JsonRecordAssert {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static void assertFlowFileRecords(final FlowFile flowFile, final KinesisClientRecord... expectedRecords) {
        assertFlowFileRecords(flowFile, List.of(expectedRecords));
    }

    static void assertFlowFileRecords(final FlowFile flowFile, final List<KinesisClientRecord> expectedRecords) {
        final List<String> expectedPayloads = expectedRecords.stream()
                .map(KinesisRecordPayload::extract)
                .toList();
        assertFlowFileRecordPayloads(flowFile, expectedPayloads);
    }

    static void assertFlowFileRecordPayloads(final FlowFile flowFile, final String... expectedPayloads) {
        assertFlowFileRecordPayloads(flowFile, List.of(expectedPayloads));
    }

    static void assertFlowFileRecordPayloads(final FlowFile flowFile, final List<String> expectedPayloads) {
        try {
            final MockFlowFile mockFlowFile = assertInstanceOf(MockFlowFile.class, flowFile, "A passed FlowFile should be an instance of MockFlowFile");

            final JsonNode node = MAPPER.readTree(mockFlowFile.getContent());
            final ArrayNode array = assertInstanceOf(ArrayNode.class, node, "FlowFile content is expected to be an array");

            assertEquals(expectedPayloads.size(), array.size(), "Array size mismatch");

            for (int i = 0; i < expectedPayloads.size(); i++) {
                final JsonNode recordNode = array.get(i);
                final JsonNode expectedNode = MAPPER.readTree(expectedPayloads.get(i));
                assertEquals(recordNode, expectedNode, "Record at index " + i + " does not match expected JSON");
            }

        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonRecordAssert() {
    }
}

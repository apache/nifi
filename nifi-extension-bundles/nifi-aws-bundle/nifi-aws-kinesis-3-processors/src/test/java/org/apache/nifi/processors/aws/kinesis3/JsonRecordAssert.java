package org.apache.nifi.processors.aws.kinesis3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
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

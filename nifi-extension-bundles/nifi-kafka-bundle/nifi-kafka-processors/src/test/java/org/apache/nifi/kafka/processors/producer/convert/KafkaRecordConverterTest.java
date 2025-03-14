package org.apache.nifi.kafka.processors.producer.convert;

import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.processors.producer.key.KeyFactory;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class KafkaRecordConverterTest {

    private static final int EXPECTED_RECORD_COUNT = 50000; // Expect 50,000 records
    private static final int MAX_MESSAGE_SIZE = 5 * 1024 * 1024; // 5MB
    private static final byte[] LARGE_SAMPLE_INPUT = new byte[MAX_MESSAGE_SIZE];
    private static final Map<String, String> SAMPLE_ATTRIBUTES = Map.of("attribute1", "value1");

    @Mock
    private HeadersFactory headersFactory;
    @Mock
    private KeyFactory keyFactory;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(headersFactory.getHeaders(any())).thenReturn(Collections.emptyList());
        when(keyFactory.getKey(any(), any())).thenReturn(new byte[]{1, 2, 3});

        // Create 5MB of sample data with multiple records
        StringBuilder sb = new StringBuilder();
        int recordCount = EXPECTED_RECORD_COUNT;
        int approximateRecordSize = MAX_MESSAGE_SIZE / recordCount;

        for (int i = 0; i < recordCount; i++) {
            sb.append("{\"id\":").append(i).append(",\"payload\":\"");
            int payloadSize = approximateRecordSize - 30;
            for (int j = 0; j < payloadSize; j++) {
                sb.append((char) ('A' + (j % 26)));
            }
            sb.append("\"}");
            if (i < recordCount - 1) {
                sb.append("\n"); // Only add newline if it's not the last record
            }
            if (sb.length() >= MAX_MESSAGE_SIZE - 100) {
                break;
            }
        }
        byte[] stringBytes = sb.toString().getBytes();
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
}
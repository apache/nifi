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
package org.apache.nifi.lookup;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@Timeout(10)
@ExtendWith(MockitoExtension.class)
class TestRestLookupServiceMarkReset {

    private MockWebServer mockWebServer;
    private RestLookupService restLookupService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RecordReaderFactory recordReaderFactory;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RecordReader recordReader;

    private static final String SERVICE_ID = RestLookupService.class.getSimpleName() + "MarkReset";
    private static final String READER_ID = RecordReaderFactory.class.getSimpleName() + "MarkReset";

    @BeforeEach
    void setUp() throws IOException, InitializationException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        restLookupService = new RestLookupService();

        when(recordReaderFactory.getIdentifier()).thenReturn(READER_ID);
        runner.addControllerService(READER_ID, recordReaderFactory);
        runner.addControllerService(SERVICE_ID, restLookupService);

        final String url = mockWebServer.url("/markreset").toString();
        runner.setProperty(restLookupService, RestLookupService.URL, url);
        runner.setProperty(restLookupService, RestLookupService.RECORD_READER, READER_ID);
        runner.enableControllerService(restLookupService);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.close();
    }

    @Test
    void testReaderUsesMarkResetBeyondBufferedStreamLimit() throws Exception {
        // Large JSON body to exceed the default BufferedInputStream buffer (8192 bytes)
        final int size = 20_000;
        String sb = '{' + "\"data\":\"" +
                "a".repeat(size) +
                "\"}";

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .body(sb)
                .build());

        // When the RecordReaderFactory is asked to create a reader, simulate mark/reset misuse
        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenAnswer(invocation -> {
            final InputStream in = invocation.getArgument(1);
            if (in.markSupported()) {
                in.mark(1); // tiny read limit
                final byte[] buffer = new byte[4096];
                long total = 0;
                int read;
                while ((read = in.read(buffer)) != -1) {
                    total += read;
                    if (total > 10_000) { // read beyond the default buffer 8192
                        break;
                    }
                }
                // This reset would have failed before the fix when using BufferedInputStream over network stream
                in.reset();
            }
            return recordReader;
        });

        // Return a simple record at once
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("ok", RecordFieldType.BOOLEAN.getDataType())));
        final Record firstRecord = new MapRecord(schema, Map.of("ok", true));
        when(recordReader.nextRecord()).thenReturn(firstRecord, (Record) null);

        final Map<String, Object> coordinates = new LinkedHashMap<>();
        final Optional<Record> result = restLookupService.lookup(coordinates);
        assertTrue(result.isPresent(), "Expected record to be present when reader performs mark/reset beyond buffer limit");
    }

    @Test
    void testLookupThrowsWhenMarkResetExceedsBufferSize() throws Exception {
        // Create a body significantly larger than the configured buffer size (8 MB)
        final int size = RestLookupService.INPUT_STREAM_BUFFER_SIZE + 50_000;
        final String body = '{' + "\"data\":\"" + "x".repeat(size) + "\"}";

        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .body(body)
                .build());

        // Simulate a RecordReader that misuses mark/reset: sets a tiny read limit,
        // reads far beyond the BufferedInputStream capacity, then attempts reset.
        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenAnswer(invocation -> {
            final InputStream in = invocation.getArgument(1);
            if (in.markSupported()) {
                in.mark(1); // Tiny read limit
                final byte[] buffer = new byte[8192];
                long total = 0;
                int read;
                while ((read = in.read(buffer)) != -1) {
                    total += read;
                    if (total > RestLookupService.INPUT_STREAM_BUFFER_SIZE + 10_000) {
                        break;
                    }
                }
                // This reset should fail since we read beyond the mark's readlimit and the buffer size
                in.reset();
            }
            return recordReader; // Not expected to be used due to exception
        });

        // Verify that the lookup wraps the reset failure into LookupFailureException
        assertThrows(LookupFailureException.class, () -> restLookupService.lookup(new LinkedHashMap<>()));
    }
}

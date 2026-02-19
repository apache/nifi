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
package org.apache.nifi.services.couchbase;

import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.services.couchbase.AbstractCouchbaseService.COUCHBASE_CONNECTION_SERVICE;
import static org.apache.nifi.services.couchbase.AbstractCouchbaseService.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CouchbaseRecordLookupServiceTest extends AbstractCouchbaseServiceTest {

    private static final String LOOKUP_SERVICE_ID = "lookupService";
    private static final String RECORD_READER_ID = "recordReaderService";

    private CouchbaseRecordLookupService lookupService;
    private CouchbaseClient client;

    @BeforeEach
    public void setup() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        lookupService = new CouchbaseRecordLookupService();
        client = mock(CouchbaseClient.class);

        final CouchbaseConnectionService connectionService = mockConnectionService(client);

        final MockRecordParser readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("key", RecordFieldType.STRING);
        readerFactory.addRecord("value");

        runner.addControllerService(CONNECTION_SERVICE_ID, connectionService);
        runner.addControllerService(RECORD_READER_ID, readerFactory);
        runner.addControllerService(LOOKUP_SERVICE_ID, lookupService);

        runner.enableControllerService(connectionService);
        runner.enableControllerService(readerFactory);

        runner.setProperty(lookupService, COUCHBASE_CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(lookupService, CouchbaseRecordLookupService.RECORD_READER, RECORD_READER_ID);

        runner.enableControllerService(lookupService);
    }

    @Test
    public void testSuccessfulLookup() throws LookupFailureException, CouchbaseException {
        when(client.getDocument(anyString())).thenReturn(new CouchbaseGetResult(TEST_DOCUMENT_CONTENT.getBytes(), TEST_CAS));

        final Map<String, Object> coordinates = Collections.singletonMap(KEY, TEST_DOCUMENT_ID);
        final Optional<Record> result = lookupService.lookup(coordinates);

        assertTrue(result.isPresent());

        final List<RecordField> fields = Collections.singletonList(new RecordField("key", RecordFieldType.STRING.getDataType()));
        final Record expectedRecord = new MapRecord(new SimpleRecordSchema(fields), Collections.singletonMap("key", "value"));

        assertEquals(expectedRecord, result.get());
    }

    @Test
    public void testLookupFailure() throws CouchbaseException {
        when(client.getDocument(anyString())).thenThrow(new CouchbaseException("Test exception"));

        final Map<String, Object> coordinates = Collections.singletonMap(KEY, TEST_DOCUMENT_ID);

        assertThrows(LookupFailureException.class, () -> lookupService.lookup(coordinates));
    }

    @Test
    public void testMissingKey() throws LookupFailureException {
        final Optional<Record> result = lookupService.lookup(Collections.emptyMap());

        assertTrue(result.isEmpty());
    }
}

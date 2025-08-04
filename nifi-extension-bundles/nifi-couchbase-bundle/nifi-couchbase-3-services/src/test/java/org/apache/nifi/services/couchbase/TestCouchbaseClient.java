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

import com.couchbase.client.java.Collection;

import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.apache.nifi.services.couchbase.utils.DocumentType.Json;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCouchbaseClient {

    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_SCOPE = "test-scope";
    private static final String TEST_COLLECTION = "test-collection";
    private static final long TEST_CAS = 1L;

    private static Collection collection;

    @BeforeAll
    static void beforeAll() {
        collection = mock(Collection.class);
        when(collection.bucketName()).thenReturn(TEST_BUCKET);
        when(collection.scopeName()).thenReturn(TEST_SCOPE);
        when(collection.name()).thenReturn(TEST_COLLECTION);
    }

    @Test
    void testPutJsonDocument() throws CouchbaseException {
        final String documentId = "test-document-id";
        final String content = "{\"key\":\"value\"}";
        final Couchbase3Client client = new Couchbase3Client(collection, Json, PersistTo.ONE, ReplicateTo.ONE);

        final MutationResult result = mock(MutationResult.class);
        when(result.cas()).thenReturn(TEST_CAS);

        when(collection.upsert(anyString(), any(), any())).thenReturn(result);

        final long cas = client.upsertDocument(documentId, content.getBytes());

        assertEquals(TEST_CAS, cas);
    }

    @Test
    void testPutJsonDocumentValidationFailure() {
        final String documentId = "test-document-id";
        final String content = "{invalid-json}";
        final Couchbase3Client client = new Couchbase3Client(collection, Json, PersistTo.ONE, ReplicateTo.ONE);

        final Exception exception = assertThrows(CouchbaseException.class, () -> client.upsertDocument(documentId, content.getBytes()));

        assertTrue(exception.getMessage().contains("The provided input is invalid."));
    }

    @Test
    void testGetDocument() throws CouchbaseException {
        final String documentId = "test-document-id";
        final Couchbase3Client client = new Couchbase3Client(collection, Json, PersistTo.ONE, ReplicateTo.ONE);

        final Instant expiryTime = Instant.now();
        final GetResult result = mock(GetResult.class);
        when(result.cas()).thenReturn(TEST_CAS);
        when(result.expiryTime()).thenReturn(Optional.of(expiryTime));

        when(collection.get(anyString(), any())).thenReturn(result);

        final CouchbaseGetResult getResult = client.getDocument(documentId);

        assertEquals(TEST_CAS, getResult.cas());
    }
}

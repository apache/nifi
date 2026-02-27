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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.services.couchbase.exception.CouchbaseDocNotFoundException;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.CouchbaseUpsertResult;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.services.couchbase.AbstractCouchbaseService.COUCHBASE_CONNECTION_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouchbaseMapCacheClientTest extends AbstractCouchbaseServiceTest {

    private final Serializer<String> stringSerializer = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private final Deserializer<String> stringDeserializer = input -> new String(input, StandardCharsets.UTF_8);
    private CouchbaseMapCacheClient mapCacheClient;
    private CouchbaseClient client;

    @BeforeEach
    void init() {
        mapCacheClient = new CouchbaseMapCacheClient();
        client = mock(CouchbaseClient.class);

        final CouchbaseConnectionService connectionService = mockConnectionService(client);
        final MockControllerServiceInitializationContext serviceInitializationContext = new MockControllerServiceInitializationContext(connectionService, CONNECTION_SERVICE_ID);
        final Map<PropertyDescriptor, String> properties = Collections.singletonMap(COUCHBASE_CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());

        mapCacheClient.onEnabled(context);
    }

    @Test
    void testCacheGet() throws CouchbaseException, IOException {
        when(client.getDocument(anyString())).thenReturn(new CouchbaseGetResult(TEST_DOCUMENT_CONTENT.getBytes(), TEST_CAS));

        final String result = mapCacheClient.get(TEST_DOCUMENT_ID, stringSerializer, stringDeserializer);

        assertEquals(TEST_DOCUMENT_CONTENT, result);
    }

    @Test
    void testCacheGetFailure() throws CouchbaseException {
        when(client.getDocument(anyString())).thenThrow(new CouchbaseException("Test exception", null));

        assertThrows(IOException.class, () -> mapCacheClient.get(TEST_DOCUMENT_ID, stringSerializer, stringDeserializer));
    }

    @Test
    void testCacheGetNotFound() throws CouchbaseException, IOException {
        when(client.getDocument(anyString())).thenThrow(new CouchbaseDocNotFoundException("Test doc not found exception", null));

        final String result = mapCacheClient.get(TEST_DOCUMENT_ID, stringSerializer, stringDeserializer);

        assertNull(result);
    }

    @Test
    void testCachePut() throws CouchbaseException, IOException {
        when(client.upsertDocument(anyString(), any())).thenReturn(new CouchbaseUpsertResult(TEST_CAS));

        mapCacheClient.put(TEST_DOCUMENT_ID, TEST_DOCUMENT_CONTENT, stringSerializer, stringSerializer);

        verify(client, times(1)).upsertDocument(eq(TEST_DOCUMENT_ID), eq(TEST_DOCUMENT_CONTENT.getBytes()));
    }

    @Test
    void testCachePutFailure() throws CouchbaseException {
        when(client.upsertDocument(anyString(), any())).thenThrow(new CouchbaseException("Test exception"));

        assertThrows(IOException.class, () -> mapCacheClient.put(TEST_DOCUMENT_ID, TEST_DOCUMENT_CONTENT, stringSerializer, stringSerializer));
    }

    @Test
    void testCacheRemove() throws CouchbaseException, IOException {
        mapCacheClient.remove(TEST_DOCUMENT_ID, stringSerializer);

        verify(client, times(1)).removeDocument(eq(TEST_DOCUMENT_ID));
    }
}

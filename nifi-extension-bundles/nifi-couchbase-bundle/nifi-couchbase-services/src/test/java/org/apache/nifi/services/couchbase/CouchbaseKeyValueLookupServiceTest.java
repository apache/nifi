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
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.services.couchbase.exception.CouchbaseDocNotFoundException;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseLookupInResult;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.services.couchbase.AbstractCouchbaseService.COUCHBASE_CONNECTION_SERVICE;
import static org.apache.nifi.services.couchbase.AbstractCouchbaseService.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CouchbaseKeyValueLookupServiceTest extends AbstractCouchbaseServiceTest {

    private CouchbaseKeyValueLookupService lookupService;
    private CouchbaseClient client;

    @BeforeEach
    public void init() {
        lookupService = new CouchbaseKeyValueLookupService();
        client = mock(CouchbaseClient.class);

        final CouchbaseConnectionService connectionService = mockConnectionService(client);
        final MockControllerServiceInitializationContext serviceInitializationContext = new MockControllerServiceInitializationContext(connectionService, CONNECTION_SERVICE_ID);
        final Map<PropertyDescriptor, String> properties = Collections.singletonMap(COUCHBASE_CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());

        lookupService.onEnabled(context);
    }

    @Test
    public void testSuccessfulLookup() throws CouchbaseException, LookupFailureException {
        when(client.lookupIn(anyString(), any())).thenReturn(new CouchbaseLookupInResult("test result", TEST_CAS));

        final Map<String, Object> coordinates = Collections.singletonMap(KEY, TEST_DOCUMENT_ID);
        final Optional<String> result = lookupService.lookup(coordinates);

        assertTrue(result.isPresent());
        assertEquals("test result", result.get());
    }

    @Test
    public void testLookupFailure() throws CouchbaseException {
        when(client.lookupIn(anyString(), any())).thenThrow(new CouchbaseException("Test exception"));

        final Map<String, Object> coordinates = Collections.singletonMap(KEY, TEST_DOCUMENT_ID);

        assertThrows(LookupFailureException.class, () -> lookupService.lookup(coordinates));
    }

    @Test
    public void testDocumentNotFoundInLookup() throws CouchbaseException, LookupFailureException {
        when(client.lookupIn(anyString(), any())).thenThrow(new CouchbaseDocNotFoundException("Test doc not found exception", null));

        final Map<String, Object> coordinates = Collections.singletonMap(KEY, TEST_DOCUMENT_ID);
        final Optional<String> result = lookupService.lookup(coordinates);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testMissingKey() throws LookupFailureException {
        final Optional<String> result = lookupService.lookup(Collections.emptyMap());

        assertTrue(result.isEmpty());
    }
}

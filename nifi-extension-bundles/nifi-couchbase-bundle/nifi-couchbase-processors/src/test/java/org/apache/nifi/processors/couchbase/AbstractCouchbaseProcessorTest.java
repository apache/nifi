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
package org.apache.nifi.processors.couchbase;

import org.apache.nifi.services.couchbase.CouchbaseClient;
import org.apache.nifi.services.couchbase.CouchbaseConnectionService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractCouchbaseProcessorTest {

    protected static final String SERVICE_ID = "couchbaseConnectionService";
    protected static final String TEST_DOCUMENT_ID = "test-document-id";
    protected static final String TEST_DOCUMENT_CONTENT = "{\"key\":\"value\"}";
    protected static final String TEST_SERVICE_LOCATION = "couchbase://test-location";
    protected static final long TEST_CAS = 1L;

    protected static CouchbaseConnectionService mockConnectionService(CouchbaseClient client) {
        final CouchbaseConnectionService connectionService = mock(CouchbaseConnectionService.class);
        when(connectionService.getIdentifier()).thenReturn(SERVICE_ID);
        when(connectionService.getClient(any())).thenReturn(client);
        when(connectionService.getServiceLocation()).thenReturn(TEST_SERVICE_LOCATION);
        return connectionService;
    }
}

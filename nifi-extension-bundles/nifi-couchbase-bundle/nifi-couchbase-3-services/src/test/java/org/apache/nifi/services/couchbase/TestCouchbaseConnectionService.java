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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;
import org.apache.nifi.services.couchbase.utils.CouchbasePutResult;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Map.entry;
import static org.apache.nifi.services.couchbase.Couchbase3ConnectionService.BUCKET_NAME;
import static org.apache.nifi.services.couchbase.Couchbase3ConnectionService.DOCUMENT_TYPE;
import static org.apache.nifi.services.couchbase.CouchbaseConnectionService.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.BUCKET_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.CAS_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.COLLECTION_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.DOCUMENT_ID_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.EXPIRY_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.CouchbaseAttributes.SCOPE_ATTRIBUTE;
import static org.apache.nifi.services.couchbase.utils.DocumentType.Json;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCouchbaseConnectionService {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_SCOPE = "test-scope";
    private static final String TEST_COLLECTION = "test-collection";
    private static final String TEST_TRANSITION_URL = "test-bucket.test-scope.test-collection.test-document-id";

    private static Couchbase3ConnectionService connectionService;
    private static Cluster cluster;
    private static Collection collection;

    @BeforeAll
    static void beforeAll() {
        final Bucket bucket = mock(Bucket.class);
        final Scope scope = mock(Scope.class);
        cluster = mock(Cluster.class);
        collection = mock(Collection.class);

        when(cluster.bucket(anyString())).thenReturn(bucket);
        when(bucket.scope(anyString())).thenReturn(scope);
        when(scope.collection(anyString())).thenReturn(collection);
        when(collection.bucketName()).thenReturn(TEST_BUCKET);
        when(collection.scopeName()).thenReturn(TEST_SCOPE);
        when(collection.name()).thenReturn(TEST_COLLECTION);

        final Couchbase3ClusterService clusterService = mock(Couchbase3ClusterService.class);
        when(clusterService.getCluster()).thenReturn(cluster);

        connectionService = new Couchbase3ConnectionService();
    }

    @Test
    void testPutJsonDocument() throws CouchbaseException {
        final String documentId = "test-document-id";
        final String content = "{\"key\":\"value\"}";
        final Couchbase3ClusterService clusterService = mock(Couchbase3ClusterService.class);
        when(clusterService.getCluster()).thenReturn(cluster);

        final MockControllerServiceInitializationContext serviceInitializationContext = new MockControllerServiceInitializationContext(clusterService, SERVICE_ID);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
        properties.put(BUCKET_NAME, TEST_BUCKET);
        properties.put(DOCUMENT_TYPE, Json.name());

        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());
        connectionService.onEnabled(context);

        final MutationResult result = mock(MutationResult.class);
        when(result.cas()).thenReturn(1L);

        when(collection.upsert(anyString(), any(), any())).thenReturn(result);

        final CouchbasePutResult putResult = connectionService.putDocument(documentId, content.getBytes());

        final Map<String, String> expectedAttributes = Map.ofEntries(
                entry(BUCKET_ATTRIBUTE, TEST_BUCKET),
                entry(SCOPE_ATTRIBUTE, TEST_SCOPE),
                entry(COLLECTION_ATTRIBUTE, TEST_COLLECTION),
                entry(DOCUMENT_ID_ATTRIBUTE, documentId),
                entry(CAS_ATTRIBUTE, String.valueOf(1L))
        );

        assertEquals(expectedAttributes, putResult.attributes());
        assertEquals(TEST_TRANSITION_URL, putResult.transitUrl());
    }

    @Test
    void testPutJsonDocumentValidationFailure() {
        final String documentId = "test-document-id";
        final String content = "{invalid-json}";
        final Couchbase3ClusterService clusterService = mock(Couchbase3ClusterService.class);
        when(clusterService.getCluster()).thenReturn(cluster);

        final MockControllerServiceInitializationContext serviceInitializationContext = new MockControllerServiceInitializationContext(clusterService, SERVICE_ID);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
        properties.put(BUCKET_NAME, TEST_BUCKET);
        properties.put(DOCUMENT_TYPE, Json.name());

        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());
        connectionService.onEnabled(context);

        final Exception exception = assertThrows(CouchbaseException.class, () -> connectionService.putDocument(documentId, content.getBytes()));

        assertTrue(exception.getMessage().contains("The provided input is invalid."));
    }

    @Test
    void testGetDocument() throws CouchbaseException {
        final String documentId = "test-document-id";
        final Couchbase3ClusterService clusterService = mock(Couchbase3ClusterService.class);
        when(clusterService.getCluster()).thenReturn(cluster);

        final MockControllerServiceInitializationContext serviceInitializationContext = new MockControllerServiceInitializationContext(clusterService, SERVICE_ID);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
        properties.put(BUCKET_NAME, TEST_BUCKET);

        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());
        connectionService.onEnabled(context);

        final Instant expiryTime = Instant.now();
        final GetResult result = mock(GetResult.class);
        when(result.cas()).thenReturn(1L);
        when(result.expiryTime()).thenReturn(Optional.of(expiryTime));

        when(collection.get(anyString(), any())).thenReturn(result);

        final CouchbaseGetResult getResult = connectionService.getDocument(documentId);

        final Map<String, String> expectedAttributes = Map.ofEntries(
                entry(BUCKET_ATTRIBUTE, TEST_BUCKET),
                entry(SCOPE_ATTRIBUTE, TEST_SCOPE),
                entry(COLLECTION_ATTRIBUTE, TEST_COLLECTION),
                entry(DOCUMENT_ID_ATTRIBUTE, documentId),
                entry(CAS_ATTRIBUTE, String.valueOf(1L)),
                entry(EXPIRY_ATTRIBUTE, expiryTime.toString()),
                entry(CoreAttributes.MIME_TYPE.key(), "application/json")
        );

        assertEquals(expectedAttributes, getResult.attributes());
        assertEquals(TEST_TRANSITION_URL, getResult.transitUrl());
    }
}

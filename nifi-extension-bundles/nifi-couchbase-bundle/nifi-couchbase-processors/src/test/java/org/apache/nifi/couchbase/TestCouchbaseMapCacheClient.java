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
package org.apache.nifi.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.GetResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.couchbase.GetCouchbaseKey;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COLLECTION_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.SCOPE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCouchbaseMapCacheClient extends AbstractCouchbaseNifiTest {

    private final Serializer<String> stringSerializer = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private final Deserializer<String> stringDeserializer = input -> new String(input, StandardCharsets.UTF_8);

    // TODO: Add more tests

    @Test
    public void testGet() throws Exception {
        final CouchbaseMapCacheClient client = new CouchbaseMapCacheClient();
        final CouchbaseClusterControllerService couchbaseService = mock(CouchbaseClusterControllerService.class);
        final Collection mcol = setupMockCouchbase(bucketName, scopeName, collectionName);
        final Bucket bucketMock = mock(Bucket.class);
        final Scope scopeMock = mock(Scope.class);
        when(bucketMock.name()).thenReturn(bucketName);
        when(bucketMock.scope(scopeName)).thenReturn(scopeMock);
        when(scopeMock.collection(collectionName)).thenReturn(mcol);

        final MockControllerServiceInitializationContext serviceInitializationContext
                = new MockControllerServiceInitializationContext(couchbaseService, "couchbaseService");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(COUCHBASE_CLUSTER_SERVICE, "couchbaseService");
        properties.put(BUCKET_NAME, bucketName);
        properties.put(SCOPE_NAME, scopeName);
        properties.put(COLLECTION_NAME, collectionName);

        final byte[] contents = "value".getBytes(StandardCharsets.UTF_8);
        when(couchbaseService.openCollection(bucketName, scopeName, collectionName)).thenReturn(mcol);
        when(couchbaseService.openBucket(bucketName)).thenReturn(bucketMock);
        when(mcol.get(eq("key"))).thenReturn(
                new GetResult(contents, 0, 200L, Optional.empty(), null)
        );

        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext, new HashMap<>());
        client.configure(context);
        final String cacheEntry = client.get("key", stringSerializer, stringDeserializer);

        assertEquals("value", cacheEntry);
    }

    @Override
    protected Class<? extends Processor> processor() {
        return GetCouchbaseKey.class; // stub
    }
}
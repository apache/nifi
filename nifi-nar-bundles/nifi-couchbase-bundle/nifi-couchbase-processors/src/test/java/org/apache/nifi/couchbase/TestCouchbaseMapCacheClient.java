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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCouchbaseMapCacheClient {

    private final Serializer<String> stringSerializer = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private final Deserializer<String> stringDeserializer = input -> new String(input, StandardCharsets.UTF_8);

    // TODO: Add more tests

    @Test
    public void testGet() throws Exception {
        final CouchbaseMapCacheClient client = new CouchbaseMapCacheClient();
        final CouchbaseClusterControllerService couchbaseService = mock(CouchbaseClusterControllerService.class);
        final Bucket bucket = mock(Bucket.class);

        final MockControllerServiceInitializationContext serviceInitializationContext
                = new MockControllerServiceInitializationContext(couchbaseService, "couchbaseService");
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(COUCHBASE_CLUSTER_SERVICE, "couchbaseService");
        properties.put(BUCKET_NAME, "bucketA");

        final ByteBuf contents = Unpooled.copiedBuffer("value".getBytes(StandardCharsets.UTF_8));
        final BinaryDocument doc = BinaryDocument.create("key", contents);
        when(couchbaseService.openBucket(eq("bucketA"))).thenReturn(bucket);
        when(bucket.get(any(BinaryDocument.class))).thenReturn(doc);

        final MockConfigurationContext context = new MockConfigurationContext(properties, serviceInitializationContext);
        client.configure(context);
        final String cacheEntry = client.get("key", stringSerializer, stringDeserializer);

        assertEquals("value", cacheEntry);
    }

}

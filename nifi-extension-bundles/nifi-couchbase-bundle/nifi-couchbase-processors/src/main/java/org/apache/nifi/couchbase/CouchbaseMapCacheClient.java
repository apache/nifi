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

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COLLECTION_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.SCOPE_NAME;

@Tags({"distributed", "cache", "map", "cluster", "couchbase"})
@CapabilityDescription("Provides the ability to communicate with a Couchbase Server cluster as a DistributedMapCacheServer." +
        " This can be used in order to share a Map between nodes in a NiFi cluster." +
        " Couchbase Server cluster can provide a high available and persistent cache storage.")
public class CouchbaseMapCacheClient extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseMapCacheClient.class);

    private CouchbaseClusterControllerService clusterService;
    private Bucket bucket;
    private Scope scope;
    private Collection collection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(COUCHBASE_CLUSTER_SERVICE);
        descriptors.add(BUCKET_NAME);
        descriptors.add(SCOPE_NAME);
        descriptors.add(COLLECTION_NAME);
        return descriptors;
    }

    @OnEnabled
    public void configure(final ConfigurationContext context) {
        clusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE).asControllerService(CouchbaseClusterControllerService.class);
        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue();
        final String scopeName = context.getProperty(SCOPE_NAME).evaluateAttributeExpressions().getValue();
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue();
        bucket = clusterService.openBucket(bucketName);
        scope = bucket.scope(scopeName);
        collection = scope.collection(collectionName);
    }

    private <K> String toDocumentId(K key, Serializer<K> keySerializer) throws IOException {
        final String docId;
        if (key instanceof String) {
            docId = (String) key;
        } else {
            // Coerce conversion from byte[] to String, this may generate unreadable String or exceed max key size.
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            keySerializer.serialize(key, bos);
            final byte[] keyBytes = bos.toByteArray();
            docId = new String(keyBytes);

        }
        return docId;
    }

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        try {
            collection.insert(docId, value, InsertOptions.insertOptions().transcoder(CouchbaseUtils.transcoder(valueSerializer, null)));
            return true;
        } catch (DocumentExistsException e) {
            return false;
        }
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        try {
            GetResult gr = collection.get(docId, GetOptions.getOptions().transcoder(CouchbaseUtils.transcoder(null, valueDeserializer)));
            return new AtomicCacheEntry<>(key, valueDeserializer.deserialize(gr.contentAsBytes()), gr.cas());
        } catch (DocumentNotFoundException dnfe) {
            return null;
        }
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        final V existing = get(key, keySerializer, valueDeserializer);
        if (existing != null) {
            return existing;
        }

        // If there's no existing value, put this value.
        if (!putIfAbsent(key, value, keySerializer, valueSerializer)) {
            // If putting this value failed, it's possible that other client has put different doc, so return that.
            return get(key, keySerializer, valueDeserializer);
        }

        // If successfully put this value, return this.
        return value;
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final Long revision = entry.getRevision().orElse(-1L);
        final String docId = toDocumentId(entry.getKey(), keySerializer);
        try {
            if (revision < 0) {
                // If the document does not exist yet, try to create one.
                try {
                    collection.insert(docId, entry.getValue(), InsertOptions.insertOptions()
                            .transcoder(CouchbaseUtils.transcoder(valueSerializer, null))
                    );
                    return true;
                } catch (DocumentExistsException e) {
                    return false;
                }
            }
            collection.replace(docId, entry.getValue(), ReplaceOptions.replaceOptions()
                    .transcoder(CouchbaseUtils.transcoder(valueSerializer, null))
                    .cas(revision)
            );
            return true;
        } catch (DocumentNotFoundException | CasMismatchException e) {
            return false;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return collection.exists(toDocumentId(key, keySerializer)).exists();
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        collection.upsert(docId, value, UpsertOptions.upsertOptions().transcoder(CouchbaseUtils.transcoder(valueSerializer, null)));
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        try {
            return valueDeserializer.deserialize(collection.get(docId).contentAsBytes());
        } catch (DocumentNotFoundException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
        try {
            collection.remove(toDocumentId(key, serializer));
            return true;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }
}

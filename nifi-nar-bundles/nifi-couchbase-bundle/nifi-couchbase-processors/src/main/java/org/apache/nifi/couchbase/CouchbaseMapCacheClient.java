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
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.Delete;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
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

import static com.couchbase.client.java.query.dsl.functions.PatternMatchingFunctions.regexpContains;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.BUCKET_NAME;
import static org.apache.nifi.couchbase.CouchbaseConfigurationProperties.COUCHBASE_CLUSTER_SERVICE;

// TODO: Doc
@Tags({"distributed", "cache", "map", "cluster", "couchbase"})
@CapabilityDescription("Provides the ability to communicate with a Couchbase Server cluster as a DistributedMapCacheServer." +
        " This can be used in order to share a Map between nodes in a NiFi cluster." +
        " Couchbase Server cluster can provide a high available and persistent cache storage.")
public class CouchbaseMapCacheClient extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    private static final Logger logger = LoggerFactory.getLogger(CouchbaseMapCacheClient.class);

    private CouchbaseClusterControllerService clusterService;
    private Bucket bucket;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(COUCHBASE_CLUSTER_SERVICE);
        descriptors.add(BUCKET_NAME);
        return descriptors;
    }

    @OnEnabled
    public void configure(final ConfigurationContext context) {
        clusterService = context.getProperty(COUCHBASE_CLUSTER_SERVICE).asControllerService(CouchbaseClusterControllerService.class);
        final String bucketName = context.getProperty(BUCKET_NAME).evaluateAttributeExpressions().getValue();
        bucket = clusterService.openBucket(bucketName);
    }

    private <V> Document toDocument(String docId, V value, Serializer<V> valueSerializer) throws IOException {
        return toDocument(docId, value, valueSerializer, 0);
    }

    private <V> Document toDocument(String docId, V value, Serializer<V> valueSerializer, long revision) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        valueSerializer.serialize(value, bos);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(bos.toByteArray());
        return BinaryDocument.create(docId, byteBuf, revision);
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
        final Document doc = toDocument(docId, value, valueSerializer);
        try {
            bucket.insert(doc);
            return true;
        } catch (DocumentAlreadyExistsException e) {
            return false;
        }
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        final BinaryDocument doc = bucket.get(BinaryDocument.create(docId));
        if (doc == null) {
            return null;
        }
        final V value = deserialize(doc, valueDeserializer);
        return new AtomicCacheEntry<>(key, value, doc.cas());
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
        final Long revision = entry.getRevision().orElse(0L);
        final String docId = toDocumentId(entry.getKey(), keySerializer);
        final Document doc = toDocument(docId, entry.getValue(), valueSerializer, revision);
        try {
            if (revision < 0) {
                // If the document does not exist yet, try to create one.
                try {
                    bucket.insert(doc);
                    return true;
                } catch (DocumentAlreadyExistsException e) {
                    return false;
                }
            }
            bucket.replace(doc);
            return true;
        } catch (DocumentDoesNotExistException|CASMismatchException e) {
            return false;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return bucket.exists(toDocumentId(key, keySerializer));
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        final Document doc = toDocument(docId, value, valueSerializer);
        bucket.upsert(doc);
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String docId = toDocumentId(key, keySerializer);
        final BinaryDocument doc = bucket.get(BinaryDocument.create(docId));
        return deserialize(doc, valueDeserializer);
    }

    private <V> V deserialize(BinaryDocument doc, Deserializer<V> valueDeserializer) throws IOException {
        if (doc == null) {
            return null;
        }
        final ByteBuf byteBuf = doc.content();
        final byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.release();
        return valueDeserializer.deserialize(bytes);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
        try {
            bucket.remove(toDocumentId(key, serializer));
            return true;
        } catch (DocumentDoesNotExistException e) {
            return false;
        }
    }

    @Override
    public long removeByPattern(String regex) throws IOException {
        Statement statement = Delete.deleteFromCurrentBucket().where(regexpContains("meta().id", regex));
        final N1qlQueryResult result = bucket.query(N1qlQuery.simple(statement));
        if (logger.isDebugEnabled()) {
            logger.debug("Deleted documents using regex {}, result={}", regex, result);
        }
        return result.info().mutationCount();
    }
}

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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.services.couchbase.exception.CouchbaseException;
import org.apache.nifi.services.couchbase.utils.CouchbaseGetResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Tags({"distributed", "cache", "map", "cluster", "couchbase"})
@CapabilityDescription("Provides the ability to communicate with a Couchbase Server cluster as a DistributedMapCacheServer." +
        " This can be used in order to share a Map between nodes in a NiFi cluster." +
        " Couchbase Server cluster can provide a high available and persistent cache storage.")
public class CouchbaseMapCacheClient extends AbstractCouchbaseService implements AtomicDistributedMapCacheClient<Long> {

    public static final PropertyDescriptor COUCHBASE_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Couchbase Connection Service")
            .description("A Couchbase Connection Service which manages connections to a Couchbase cluster.")
            .required(true)
            .identifiesControllerService(CouchbaseConnectionService.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUCHBASE_CONNECTION_SERVICE,
            BUCKET_NAME,
            SCOPE_NAME,
            COLLECTION_NAME
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String documentId = serializeDocumentKey(key, keySerializer);
        try {
            final CouchbaseGetResult result = couchbaseClient.getDocument(documentId);
            return new AtomicCacheEntry<>(key, deserializeDocument(valueDeserializer, result.resultContent()), result.cas());
        } catch (CouchbaseException e) {
            return null;
        }
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String documentId = serializeDocumentKey(entry.getKey(), keySerializer);
        final byte[] document = serializeDocument(entry.getValue(), valueSerializer);

        try {
            couchbaseClient.replaceDocument(documentId, document);
            return true;
        } catch (CouchbaseException e) {
            return false;
        }
    }

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String documentId = serializeDocumentKey(key, keySerializer);
        final byte[] document = serializeDocument(value, valueSerializer);

        try {
            couchbaseClient.insertDocument(documentId, document);
            return true;
        } catch (CouchbaseException e) {
            return false;
        }
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        if (containsKey(key, keySerializer)) {
            return get(key, keySerializer, valueDeserializer);
        }

        put(key, value, keySerializer, valueSerializer);
        return value;
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        final String documentId = serializeDocumentKey(key, keySerializer);

        try {
            return couchbaseClient.documentExists(documentId);
        } catch (CouchbaseException e) {
            throw new IOException(e);
        }
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final String documentId = serializeDocumentKey(key, keySerializer);
        final byte[] document = serializeDocument(value, valueSerializer);

        try {
            couchbaseClient.upsertDocument(documentId, document);
        } catch (CouchbaseException e) {
            throw new IOException(e);
        }
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final String documentId = serializeDocumentKey(key, keySerializer);

        try {
            final CouchbaseGetResult result = couchbaseClient.getDocument(documentId);
            return deserializeDocument(valueDeserializer, result.resultContent());
        } catch (CouchbaseException e) {
            return null;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
        final String documentId = serializeDocumentKey(key, serializer);

        try {
            couchbaseClient.removeDocument(documentId);
            return true;
        } catch (CouchbaseException e) {
            return false;
        }
    }

    private <S> String serializeDocumentKey(final S key, final Serializer<S> serializer) throws IOException {
        final String result;

        if (key instanceof String) {
            result = (String) key;
        } else {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            serializer.serialize(key, stream);
            result = stream.toString(StandardCharsets.UTF_8);
        }

        if (result.isEmpty()) {
            throw new IOException("Cache record key cannot be empty!");
        }

        return result;
    }

    private <S> byte[] serializeDocument(final S value, final Serializer<S> serializer) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        serializer.serialize(value, stream);
        return stream.toByteArray();
    }

    private static <V> V deserializeDocument(final Deserializer<V> deserializer, final byte[] value) throws IOException {
        return deserializer.deserialize(value);
    }
}

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

package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.ListableEntity;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestAbstractListProcessor {

    @Test
    public void testOnlyNewEntriesEmitted() {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", 1492L);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id3", 1491L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id", 1494L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testStateStoredInDistributedService() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        runner.run();

        proc.addEntity("name", "id", 1492L);
        runner.run();

        assertEquals(1, cache.stored.size());
    }

    @Test
    public void testFetchOnStart() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        runner.run();

        assertEquals(1, cache.fetchCount);
    }

    private static class DistributedCache extends AbstractControllerService implements DistributedMapCacheClient {
        private final Map<Object, Object> stored = new HashMap<>();
        private int fetchCount = 0;

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
            return null;
        }

        @Override
        public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            stored.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            fetchCount++;
            return (V) stored.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
            final Object value = stored.remove(key);
            return value != null;
        }
    }


    private static class ConcreteListProcessor extends AbstractListProcessor<ListableEntity> {
        private final List<ListableEntity> entities = new ArrayList<>();

        @Override
        protected File getPersistenceFile() {
            return new File("target/ListProcessor-local-state.json");
        }

        public void addEntity(final String name, final String identifier, final long timestamp) {
            final ListableEntity entity = new ListableEntity() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public String getIdentifier() {
                    return identifier;
                }

                @Override
                public long getTimestamp() {
                    return timestamp;
                }
            };

            entities.add(entity);
        }

        @Override
        protected Map<String, String> createAttributes(final ListableEntity entity, final ProcessContext context) {
            return Collections.emptyMap();
        }

        @Override
        protected String getPath(final ProcessContext context) {
            return "/path";
        }

        @Override
        protected List<ListableEntity> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
            return Collections.unmodifiableList(entities);
        }

        @Override
        protected boolean isListingResetNecessary(PropertyDescriptor property) {
            return false;
        }
    }
}

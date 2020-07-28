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
package org.apache.nifi.lookup;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestDistributedMapCacheLookupService {

    final static Optional<String> EMPTY_STRING = Optional.empty();

    @Test
    public void testDistributedMapCacheLookupService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DistributedMapCacheLookupService service = new DistributedMapCacheLookupService();
        final DistributedMapCacheClient client = new DistributedMapCacheClientImpl();

        runner.addControllerService("client", client);
        runner.addControllerService("lookup-service", service);
        runner.setProperty(service, DistributedMapCacheLookupService.PROP_DISTRIBUTED_CACHE_SERVICE, "client");

        runner.enableControllerService(client);
        runner.enableControllerService(service);

        runner.assertValid(service);

        assertThat(service, instanceOf(LookupService.class));

        final Optional<String> get = service.lookup(Collections.singletonMap("key", "myKey"));
        assertEquals(Optional.of("myValue"), get);

        final Optional<String> absent = service.lookup(Collections.singletonMap("key", "absentKey"));
        assertEquals(EMPTY_STRING, absent);
    }

    static final class DistributedMapCacheClientImpl extends AbstractControllerService implements DistributedMapCacheClient {

        private Map<String, String> map = new HashMap<String, String>();

        @OnEnabled
        public void onEnabled(final ConfigurationContext context) {
            map.put("myKey", "myValue");
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        }

        @Override
        protected java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return new ArrayList<>();
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                final Deserializer<V> valueDeserializer) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            return (V) map.get(key);
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }
    }

}

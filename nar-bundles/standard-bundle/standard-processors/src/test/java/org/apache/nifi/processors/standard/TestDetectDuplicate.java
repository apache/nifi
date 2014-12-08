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

import org.apache.nifi.processors.standard.DetectDuplicate;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.apache.commons.lang.SerializationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDetectDuplicate {

    private static Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.DetectDuplicate", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestDetectDuplicate", "debug");
        LOGGER = LoggerFactory.getLogger(TestListenUDP.class);
    }

    @Test
    public void testDuplicate() throws InitializationException {

        TestRunner runner = TestRunners.newTestRunner(DetectDuplicate.class);
        final DistributedMapCacheClientImpl client = createClient();
        final Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), "localhost");
        runner.addControllerService("client", client, clientProperties);
        runner.setProperty(DetectDuplicate.DISTRIBUTED_CACHE_SERVICE, "client");
        runner.setProperty(DetectDuplicate.FLOWFILE_DESCRIPTION, "The original flow file");
        runner.setProperty(DetectDuplicate.AGE_OFF_DURATION, "48 hours");
        Map<String, String> props = new HashMap<>();
        props.put("hash.value", "1000");
        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(DetectDuplicate.REL_NON_DUPLICATE, 1);
        runner.clearTransferState();
        client.exists = true;
        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(DetectDuplicate.REL_DUPLICATE, 1);
        runner.assertTransferCount(DetectDuplicate.REL_NON_DUPLICATE, 0);
        runner.assertTransferCount(DetectDuplicate.REL_FAILURE, 0);
    }

    @Test
    public void testDuplicateWithAgeOff() throws InitializationException, InterruptedException {

        TestRunner runner = TestRunners.newTestRunner(DetectDuplicate.class);
        final DistributedMapCacheClientImpl client = createClient();
        final Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), "localhost");
        runner.addControllerService("client", client, clientProperties);
        runner.setProperty(DetectDuplicate.DISTRIBUTED_CACHE_SERVICE, "client");
        runner.setProperty(DetectDuplicate.FLOWFILE_DESCRIPTION, "The original flow file");
        runner.setProperty(DetectDuplicate.AGE_OFF_DURATION, "2 secs");
        Map<String, String> props = new HashMap<>();
        props.put("hash.value", "1000");
        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(DetectDuplicate.REL_NON_DUPLICATE, 1);
        runner.clearTransferState();
        client.exists = true;
        Thread.sleep(3000);
        runner.enqueue(new byte[]{}, props);
        runner.run();
        runner.assertAllFlowFilesTransferred(DetectDuplicate.REL_NON_DUPLICATE, 1);
        runner.assertTransferCount(DetectDuplicate.REL_DUPLICATE, 0);
        runner.assertTransferCount(DetectDuplicate.REL_FAILURE, 0);
    }

    private DistributedMapCacheClientImpl createClient() throws InitializationException {

        final DistributedMapCacheClientImpl client = new DistributedMapCacheClientImpl();
        MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        return client;
    }

    static final class DistributedMapCacheClientImpl extends AbstractControllerService implements DistributedMapCacheClient {

        boolean exists = false;
        private Object cacheValue;

        @Override
        public void close() throws IOException {
        }

        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        }

        @Override
        protected java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            List<PropertyDescriptor> props = new ArrayList<>();
            props.add(DistributedMapCacheClientService.HOSTNAME);
            props.add(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT);
            props.add(DistributedMapCacheClientService.PORT);
            props.add(DistributedMapCacheClientService.SSL_CONTEXT_SERVICE);
            return props;
        }

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            if (exists) {
                return false;
            }

            cacheValue = value;
            return true;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                Deserializer<V> valueDeserializer) throws IOException {
            if (exists) {
                return (V) cacheValue;
            }
            cacheValue = value;
            return null;
        }

        @Override
        public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
            return exists;
        }

        @Override
        public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            return null;
        }

        @Override
        public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
            exists = false;
            return true;
        }
    }

    private static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream output) throws SerializationException, IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void deleteRecursively(final File dataFile) throws IOException {
        if (dataFile == null || !dataFile.exists()) {
            return;
        }

        final File[] children = dataFile.listFiles();
        for (final File child : children) {
            if (child.isDirectory()) {
                deleteRecursively(child);
            } else {
                for (int i = 0; i < 100 && child.exists(); i++) {
                    child.delete();
                }

                if (child.exists()) {
                    throw new IOException("Could not delete " + dataFile.getAbsolutePath());
                }
            }
        }
    }
}

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
package org.apache.nifi.hazelcast.services.cachemanager;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hazelcast.services.DummyStringSerializer;
import org.apache.nifi.hazelcast.services.cacheclient.HazelcastMapCacheClient;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TestHazelcastProcessor extends AbstractProcessor {
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";

    private static final DummyStringSerializer SERIALIZER = new DummyStringSerializer();

    public static final PropertyDescriptor TEST_HAZELCAST_MAP_CACHE_CLIENT = new PropertyDescriptor.Builder()
            .name("test-hazelcast-map-cache-client")
            .displayName("Test Hazelcast Map Cache Client")
            .identifiesControllerService(HazelcastMapCacheClient.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(TEST_HAZELCAST_MAP_CACHE_CLIENT);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final HazelcastMapCacheClient testSubject = context.getProperty(TEST_HAZELCAST_MAP_CACHE_CLIENT).asControllerService(HazelcastMapCacheClient.class);

        try {
            Assert.assertFalse(testSubject.containsKey(KEY_1, SERIALIZER));
            testSubject.put(KEY_1, VALUE_1, SERIALIZER, SERIALIZER);
            Assert.assertTrue(testSubject.containsKey(KEY_1, SERIALIZER));
            Assert.assertEquals(VALUE_1, testSubject.get(KEY_1, SERIALIZER, SERIALIZER));
            Assert.assertTrue(testSubject.remove(KEY_1, SERIALIZER));
            Assert.assertFalse(testSubject.containsKey(KEY_1, SERIALIZER));

            Assert.assertNull(testSubject.getAndPutIfAbsent(KEY_2, VALUE_2, SERIALIZER, SERIALIZER, SERIALIZER));
            Assert.assertEquals(VALUE_2, testSubject.getAndPutIfAbsent(KEY_2, VALUE_2, SERIALIZER, SERIALIZER, SERIALIZER));
            testSubject.put(KEY_1, VALUE_1, SERIALIZER, SERIALIZER);

            Assert.assertTrue(testSubject.containsKey(KEY_1, SERIALIZER));
            Assert.assertTrue(testSubject.containsKey(KEY_2, SERIALIZER));

            Assert.assertEquals(2, testSubject.removeByPattern("key.*"));

            Assert.assertTrue(testSubject.replace(new AtomicCacheEntry<>(KEY_1, VALUE_1, 0L), SERIALIZER, SERIALIZER));
            Assert.assertEquals(VALUE_1, testSubject.fetch(KEY_1, SERIALIZER, SERIALIZER).getValue());

            session.transfer(flowFile, REL_SUCCESS);
        } catch (final AssertionError| IOException e) {
            session.transfer(flowFile, REL_FAILURE);
            e.printStackTrace();
        }
    }
}

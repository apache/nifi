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
package org.apache.nifi.hazelcast.services.cacheclient;


import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hazelcast.services.DummyStringSerializer;
import org.apache.nifi.hazelcast.services.cache.HashMapHazelcastCache;
import org.apache.nifi.hazelcast.services.cachemanager.HazelcastCacheManager;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class HazelcastMapCacheClientTest {

    private final static long TTL = 0;
    private final static String CACHE_NAME = "cache";
    private final static String KEY = "key";
    private final static String VALUE = "lorem ipsum";
    private final static String VALUE_2 = "lorem ipsum dolor sit amet";
    private final static String VALUE_3 = "cras ac felis tincidunt";

    private final static DummyStringSerializer SERIALIZER = new DummyStringSerializer();

    @Mock
    private HazelcastCacheManager hazelcastCacheService;

    @Mock
    private ConfigurationContext configurationContext;

    @Mock
    private ControllerServiceInitializationContext initializationContext;

    @Mock
    private PropertyValue propertyValueForTTL;

    @Mock
    private PropertyValue propertyValueForCacheName;

    @Mock
    private PropertyValue propertyValueForCacheNameEvaluated;

    @Mock
    private PropertyValue propertyValueForConnectionService;

    private HashMapHazelcastCache cache;
    private HazelcastMapCacheClient testSubject;

    @Before
    public void setUp() throws Exception {
        cache = new HashMapHazelcastCache(CACHE_NAME);

        Mockito.when(propertyValueForTTL.asTimePeriod(TimeUnit.MILLISECONDS)).thenReturn(TTL);
        Mockito.when(propertyValueForCacheName.evaluateAttributeExpressions()).thenReturn(propertyValueForCacheNameEvaluated);
        Mockito.when(propertyValueForCacheNameEvaluated.getValue()).thenReturn(CACHE_NAME);
        Mockito.when(propertyValueForConnectionService.asControllerService(HazelcastCacheManager.class)).thenReturn(hazelcastCacheService);

        Mockito.when(configurationContext.getProperty(HazelcastMapCacheClient.HAZELCAST_CACHE_MANAGER)).thenReturn(propertyValueForConnectionService);
        Mockito.when(configurationContext.getProperty(HazelcastMapCacheClient.HAZELCAST_CACHE_NAME)).thenReturn(propertyValueForCacheName);
        Mockito.when(configurationContext.getProperty(HazelcastMapCacheClient.HAZELCAST_ENTRY_TTL)).thenReturn(propertyValueForTTL);

        Mockito.when(hazelcastCacheService.getCache(CACHE_NAME, TTL)).thenReturn(cache);
        Mockito.when(initializationContext.getLogger()).thenReturn(Mockito.mock(ComponentLog.class));

        testSubject = new HazelcastMapCacheClient();
        testSubject.initialize(initializationContext);
        testSubject.onEnabled(configurationContext);
    }

    @Test
    public void testWhenReadingDataBackItDoesNotChange() throws Exception {
        // when
        thenEntryIsNotInCache();
        whenPutEntry(VALUE);

        // then
        thenEntryIsInCache();
        thenGetEntryEquals(VALUE);
    }

    @Test
    public void testRemoveEntry() throws Exception {
        // given
        whenRemoveEntryIsUnsuccessful();

        // when
        whenPutEntry(VALUE);

        // then
        thenEntryIsInCache();
        whenRemoveEntryIsSuccessful();
        thenEntryIsNotInCache();
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        // given
        thenEntryIsNotInCache();

        // when
        whenPutIfAbsentIsSuccessful(VALUE);

        // then
        thenGetEntryEquals(VALUE);

        // when
        whenPutIfAbsentIsFailed(VALUE_2);

        // then
        thenGetEntryEquals(VALUE);
    }

    @Test
    public void testGetAndPutIfAbsent() throws Exception {
        // given
        thenEntryIsNotInCache();

        // when
        Assert.assertNull(whenGetAndPutIfAbsent(VALUE));

        // then
        thenGetEntryEquals(VALUE);

        // when
        Assert.assertEquals(VALUE, whenGetAndPutIfAbsent(VALUE_2));

        // then
        thenGetEntryEquals(VALUE);
    }

    @Test
    public void testRemoveByPattern() throws Exception {
        // given
        whenPutEntry("key1", "a");
        whenPutEntry("key2", "b");
        whenPutEntry("key3", "c");
        whenPutEntry("other", "d");

        // when
        testSubject.removeByPattern("key.*");

        // then
        thenEntryIsNotInCache("key1");
        thenEntryIsNotInCache("key2");
        thenEntryIsNotInCache("key3");
        thenEntryIsInCache("other");
    }

    @Test
    public void testWhenReplaceNonExistingAtomicEntry() throws Exception {
        // given
        thenFetchedEntryIsNull();

        // when
        whenReplaceEntryIsFailed(5L, VALUE);
        thenEntryIsNotLocked();

        // then
        thenFetchedEntryIsNull();
    }

    @Test
    public void testReplacingNonExistingAtomicEntry() throws Exception {
        // given
        whenReplaceEntryIsSuccessful(0L, VALUE);

        // then
        thenEntryIsInCache();
        thenFetchedEntryEquals(1L, VALUE);

        // then
        thenGetEntryEquals(VALUE);
    }

    @Test
    public void testReplacingExistingAtomicEntry() throws Exception {
        // given
        whenReplaceEntryIsSuccessful(0L, VALUE);
        thenEntryIsNotLocked();

        // when
        whenReplaceEntryIsSuccessful(1L, VALUE_2);
        thenEntryIsNotLocked();

        // then
        thenFetchedEntryEquals(2L, VALUE_2);

        // when & then - replace with too low version fails
        whenReplaceEntryIsFailed(1L, VALUE_3);
        thenFetchedEntryEquals(2L, VALUE_2);

        // when & then - replace with too high version fails
        whenReplaceEntryIsFailed(5L, VALUE_3);
        thenFetchedEntryEquals(2L, VALUE_2);
    }

    @Test
    public void testStartVersionWithNull() throws Exception {
        // given
        whenReplaceEntryIsSuccessful(null, VALUE);

        // when & then
        whenReplaceEntryIsSuccessful(1L, VALUE_2);
    }

    @Test
    public void testOverrideVersionedEntryWithPut() throws Exception {
        // given
        whenReplaceEntryIsSuccessful(0L, VALUE);
        whenReplaceEntryIsSuccessful(1L, VALUE_2);
        whenReplaceEntryIsSuccessful(2L, VALUE_3);

        // when
        whenPutEntry(KEY, VALUE);

        // then - version number is increased
        thenFetchedEntryEquals(1L, VALUE);
    }

    @Test
    public void testReplacingWithTheSame() throws Exception {
        // given
        whenReplaceEntryIsSuccessful(0L, VALUE);

        // when
        whenReplaceEntryIsSuccessful(1L, VALUE);

        // then - version number is increased
        thenFetchedEntryEquals(2L, VALUE);
    }

    @Test
    public void testReplacingExistingEntryAddedWithPut() throws Exception {
        // given
        whenPutEntry(VALUE);

        // then
        thenFetchedEntryEquals(1L, VALUE);

        // when
        whenReplaceEntryIsSuccessful(1L, VALUE_2);

        // then
        thenFetchedEntryEquals(2L, VALUE_2);
    }

    @Test
    public void testMultiplePutsWillHaveNoAffectOnVersion() throws Exception {
        // given
        whenPutEntry(VALUE);

        // when
        whenPutEntry(VALUE_2);

        // then
        thenGetEntryEquals(VALUE_2);
        thenFetchedEntryEquals(1L, VALUE_2);
    }

    @Test
    public void testSerialization() throws Exception {
        // given
        final Long key = 1L;
        final Double value = 1.2;

        final Serializer<Long> keySerializer = (x, output) -> output.write(x.toString().getBytes(StandardCharsets.UTF_8));
        final Serializer<Double> valueSerializer = (x, output) -> output.write(x.toString().getBytes(StandardCharsets.UTF_8));
        final Deserializer<Double> valueDeserializer = input -> Double.valueOf(new String(input, StandardCharsets.UTF_8));

        // when
        testSubject.put(key, value, keySerializer, valueSerializer);
        final Double result = testSubject.get(key, keySerializer, valueDeserializer);

        // then
        Assert.assertEquals(value, result);
    }

    private void whenRemoveEntryIsSuccessful() throws IOException {
        Assert.assertTrue(testSubject.remove(KEY, SERIALIZER));
    }

    private void whenRemoveEntryIsUnsuccessful() throws IOException {
        Assert.assertFalse(testSubject.remove(KEY, SERIALIZER));
    }

    private void whenPutEntry(final String value) throws IOException {
        whenPutEntry(KEY, value);
    }

    private void whenPutEntry(final String key, final String value) throws IOException {
        testSubject.put(key, value, SERIALIZER, SERIALIZER);
    }

    private void whenPutIfAbsentIsSuccessful(final String value) throws IOException {
        Assert.assertTrue(testSubject.putIfAbsent(KEY, value, SERIALIZER, SERIALIZER));
    }

    private void whenPutIfAbsentIsFailed(final String value) throws IOException {
        Assert.assertFalse(testSubject.putIfAbsent(KEY, value, SERIALIZER, SERIALIZER));
    }

    private String whenGetAndPutIfAbsent(final String value) throws IOException {
        return testSubject.getAndPutIfAbsent(KEY, value, SERIALIZER, SERIALIZER, SERIALIZER);
    }

    private void whenReplaceEntryIsSuccessful(final Long version, final String newValue) throws IOException {
        final AtomicCacheEntry<String, String, Long> cacheEntry = new AtomicCacheEntry<>(KEY, newValue, version);
        Assert.assertTrue(testSubject.replace(cacheEntry, SERIALIZER, SERIALIZER));
    }

    private void whenReplaceEntryIsFailed(final Long version, final String newValue) throws IOException {
        final AtomicCacheEntry<String, String, Long> cacheEntry = new AtomicCacheEntry<>(KEY, newValue, version);
        Assert.assertFalse(testSubject.replace(cacheEntry, SERIALIZER, SERIALIZER));
    }

    private void thenEntryIsNotInCache(final String key) throws IOException {
        Assert.assertFalse(testSubject.containsKey(key, SERIALIZER));
    }

    private void thenEntryIsNotInCache() throws IOException {
        thenEntryIsNotInCache(KEY);
    }

    private void thenEntryIsInCache(final String key) throws IOException {
        Assert.assertTrue(testSubject.containsKey(key, SERIALIZER));
    }

    private void thenEntryIsInCache() throws IOException {
        thenEntryIsInCache(KEY);
    }

    private void thenFetchedEntryIsNull() throws Exception {
        Assert.assertNull(testSubject.fetch(KEY, SERIALIZER, SERIALIZER));
    }

    private void thenFetchedEntryEquals(final long version, final String value) throws IOException {
        final AtomicCacheEntry<String, String, Long> result = testSubject.fetch(KEY, SERIALIZER, SERIALIZER);
        Assert.assertNotNull(result);
        Assert.assertEquals(version, result.getRevision().get().longValue());
        Assert.assertEquals(KEY, result.getKey());
        Assert.assertEquals(value, result.getValue());
    }

    private void thenGetEntryEquals(final String value) throws IOException {
        Assert.assertEquals(value, testSubject.get(KEY, SERIALIZER, SERIALIZER));
    }

    private void thenEntryIsNotLocked() {
        Assert.assertFalse(cache.getLockedEntries().contains(KEY));
    }
}
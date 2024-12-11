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
package org.apache.nifi.hazelcast.services.cache;

import com.hazelcast.map.IMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class IMapBasedHazelcastCacheTest {
    private static final String KEY = "key";
    private static final String KEY_2 = "key2";
    private static final byte[] VALUE = "value".getBytes();
    private static final byte[] VALUE_2 = "value2".getBytes();
    private static final long TTL = 5;

    @Mock
    private IMap<String, byte[]> storage;

    private IMapBasedHazelcastCache testSubject;

    @BeforeEach
    public void setUp() {
        testSubject = new IMapBasedHazelcastCache(storage, TTL);
    }

    @Test
    void testKeySet() {
        // given
        final Set<String> keys = Set.of(KEY, KEY_2);
        Mockito.when(storage.keySet()).thenReturn(keys);

        // when
        final Set<String> result = testSubject.keySet();

        // then
        Mockito.verify(storage).keySet();
        assertEquals(keys, result);
    }

    @Test
    public void testGet() {
        // given
        Mockito.when(storage.get(Mockito.anyString())).thenReturn(VALUE);

        // when
        final byte[] result = testSubject.get(KEY);

        // then
        Mockito.verify(storage).get(KEY);
        assertEquals(VALUE, result);
    }

    @Test
    public void testPutIfAbsent() {
        // given
        Mockito.when(storage.putIfAbsent(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(VALUE_2);

        // when
        final byte[] result = testSubject.putIfAbsent(KEY, VALUE);

        // then
        Mockito.verify(storage).putIfAbsent(KEY, VALUE, TTL, TimeUnit.MILLISECONDS);
        assertEquals(VALUE_2, result);
    }

    @Test
    public void testPut() {
        // when
        testSubject.put(KEY, VALUE);

        // then
        Mockito.verify(storage).put(KEY, VALUE, TTL, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testContains() {
        // given
        Mockito.when(storage.containsKey(Mockito.anyString())).thenReturn(true);

        // when
        final boolean result = testSubject.contains(KEY);

        // then
        Mockito.verify(storage).containsKey(KEY);
        assertTrue(result);
    }

    @Test
    public void testRemoveWhenExists() {
        // given
        Mockito.when(storage.remove(Mockito.anyString())).thenReturn(VALUE);

        // when
        final boolean result = testSubject.remove(KEY);

        // then
        Mockito.verify(storage).remove(KEY);
        assertTrue(result);
    }

    @Test
    public void testRemoveWhenDoesNotExist() {
        // given
        Mockito.when(storage.remove(Mockito.anyString())).thenReturn(null);

        // when
        final boolean result = testSubject.remove(KEY);

        // then
        Mockito.verify(storage).remove(KEY);
        assertFalse(result);
    }


    @Test
    public void testRemoveAll() {
        // given
        Mockito.when(storage.keySet()).thenReturn(new HashSet<>(Arrays.asList(KEY, KEY_2)));

        // when
        final int result = testSubject.removeAll(s -> true);

        // then
        Mockito.verify(storage).delete(KEY);
        Mockito.verify(storage).delete(KEY_2);
        assertEquals(2, result);
    }
}

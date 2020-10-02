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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class IMapBasedHazelcastCacheTest {
    private static final String NAME = "loremIpsum";
    private static final String KEY = "key";
    private static final String KEY_2 = "key2";
    private static final byte[] VALUE = "value".getBytes();
    private static final byte[] VALUE_2 = "value2".getBytes();
    private static final long TTL = 5;

    @Mock
    private IMap<String, byte[]> repository;

    private IMapBasedHazelcastCache testSubject;

    @Before
    public void setUp() {
        testSubject = new IMapBasedHazelcastCache(NAME, TTL, repository);
    }

    @Test
    public void testGet() {
        // given
        Mockito.when(repository.get(Mockito.anyString())).thenReturn(VALUE);

        // when
        final byte[] result = testSubject.get(KEY);

        // then
        Mockito.verify(repository).get(KEY);
        Assert.assertEquals(VALUE, result);
    }

    @Test
    public void testPutIfAbsent() {
        // given
        Mockito.when(repository.putIfAbsent(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(VALUE_2);

        // when
        final byte[] result = testSubject.putIfAbsent(KEY, VALUE);

        // then
        Mockito.verify(repository).putIfAbsent(KEY, VALUE, TTL, TimeUnit.MILLISECONDS);
        Assert.assertEquals(VALUE_2, result);
    }

    @Test
    public void testPut() {
        // when
        testSubject.put(KEY, VALUE);

        // then
        Mockito.verify(repository).put(KEY, VALUE, TTL, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testContains() {
        // given
        Mockito.when(repository.containsKey(Mockito.anyString())).thenReturn(true);

        // when
        final boolean result = testSubject.contains(KEY);

        // then
        Mockito.verify(repository).containsKey(KEY);
        Assert.assertTrue(result);
    }

    @Test
    public void testRemoveWhenExists() {
        // given
        Mockito.when(repository.remove(Mockito.anyString())).thenReturn(VALUE);

        // when
        final boolean result = testSubject.remove(KEY);

        // then
        Mockito.verify(repository).remove(KEY);
        Assert.assertTrue(result);
    }

    @Test
    public void testRemoveWhenDoesNotExist() {
        // given
        Mockito.when(repository.remove(Mockito.anyString())).thenReturn(null);

        // when
        final boolean result = testSubject.remove(KEY);

        // then
        Mockito.verify(repository).remove(KEY);
        Assert.assertFalse(result);
    }


    @Test
    public void testRemoveAll() {
        // given
        Mockito.when(repository.keySet()).thenReturn(new HashSet<>(Arrays.asList(KEY, KEY_2)));

        // when
        final int result = testSubject.removeAll(s -> true);

        // then
        Mockito.verify(repository).delete(KEY);
        Mockito.verify(repository).delete(KEY_2);
        Assert.assertEquals(2, result);
    }
}

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
package org.apache.nifi.pulsar.cache;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("rawtypes")
public class LRUCacheTest {

    @Mock
    private Producer mockedPulsarProducer;

    @Before
    public void setUp() throws InterruptedException {
      mockedPulsarProducer = mock(Producer.class);
    }

    /**
     * Make sure the LRUCache functions as a Map
     */
    @Test
    public void simpleTest() {
      LRUCache<String, Producer> cache = new LRUCache<String, Producer>(10);

      for (Character i='A'; i<='E'; i++){
         cache.put(i.toString(), mockedPulsarProducer);
      }

      assertEquals(5, cache.getSize());

      for (Character i='A'; i<='E'; i++){
         assertNotNull( cache.get(i.toString()));
      }
     }

    @Test
    public void evictionTest() {

      LRUCache<String, Producer> cache = new LRUCache<String, Producer>(5);

      for (Character i='A'; i<='Z'; i++){
         cache.put(i.toString(), mockedPulsarProducer);
      }

      // Make sure we only have 5 items in the cache
      assertEquals(5, cache.getSize());

      // Make sure we have the last 5 items added to the cache
      for (Character i='V'; i<='Z'; i++){
         assertNotNull( cache.get(i.toString()));
      }
    }

    @Test
    public void evictionLruTest() {

      LRUCache<String, Producer> cache = new LRUCache<String, Producer>(5);

      final Character A = 'A';

      // Write 25 items to the cache, and the letter 'A' every other put.
      for (Character i='B'; i<='Z'; i++){
         cache.put(i.toString(), mockedPulsarProducer);
         cache.put(A.toString(), mockedPulsarProducer);
      }

      // Make sure we only have 5 items in the cache
      assertEquals(5, cache.getSize());

      // Make sure that the letter 'A' is still in the cache due to frequent access
      assertNotNull( cache.get(A.toString()) );

      // Make sure we have the last 4 items added to the cache
      for (Character i='W'; i<='Z'; i++){
         assertNotNull( cache.get(i.toString()));
      }
    }

    @Test
    public void clearTest() throws PulsarClientException {
       LRUCache<String, Producer> cache = new LRUCache<String, Producer>(26);

       for (Character i='A'; i<='Z'; i++) {
          cache.put(i.toString(), mockedPulsarProducer);
       }

       // Make sure we only have all the items in the cache
       assertEquals(26, cache.getSize());
       cache.clear();

       verify(mockedPulsarProducer, times(26)).close();

       // Make sure all the items were removed
       assertEquals(0, cache.getSize());
    }
}

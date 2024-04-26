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
package org.apache.nifi.processor.util.listen.queue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TrackingLinkedBlockingQueueTest {
    private static final String ELEMENT = String.class.getSimpleName();

    private static final long OFFER_TIMEOUT = 1;

    private static final int INITIAL_CAPACITY = 1;

    @Test
    public void testAddRemoveGetLargestSize() {
        final TrackingLinkedBlockingQueue<String> queue = new TrackingLinkedBlockingQueue<>(INITIAL_CAPACITY);

        final boolean added = queue.add(ELEMENT);
        assertTrue(added);

        final int largestSize = queue.getLargestSize();
        assertEquals(queue.size(), largestSize);

        final boolean removed = queue.remove(ELEMENT);
        assertTrue(removed);

        assertEquals(largestSize, queue.getLargestSize());
    }

    @Test
    public void testOfferGetLargestSize() {
        final TrackingLinkedBlockingQueue<String> queue = new TrackingLinkedBlockingQueue<>();

        final boolean success = queue.offer(ELEMENT);
        assertTrue(success);

        assertEquals(queue.size(), queue.getLargestSize());
    }

    @Test
    public void testOfferTimeoutGetLargestSize() throws InterruptedException {
        final TrackingLinkedBlockingQueue<String> queue = new TrackingLinkedBlockingQueue<>();

        final boolean success = queue.offer(ELEMENT, OFFER_TIMEOUT, TimeUnit.SECONDS);
        assertTrue(success);

        assertEquals(queue.size(), queue.getLargestSize());
    }

    @Test
    public void testPutGetLargestSize() throws InterruptedException {
        final TrackingLinkedBlockingQueue<String> queue = new TrackingLinkedBlockingQueue<>();

        queue.put(ELEMENT);

        assertEquals(queue.size(), queue.getLargestSize());
    }
}

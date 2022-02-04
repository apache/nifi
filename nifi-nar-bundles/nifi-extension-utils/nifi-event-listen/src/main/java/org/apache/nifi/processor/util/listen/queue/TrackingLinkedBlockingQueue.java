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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracking extension of LinkedBlockingQueue to provide additional statistics
 */
public class TrackingLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private final AtomicInteger largestSize = new AtomicInteger();

    public TrackingLinkedBlockingQueue() {
        super();
    }

    public TrackingLinkedBlockingQueue(final int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(final E element) {
        final boolean success = super.offer(element);
        if (success) {
            updateLargestSize();
        }
        return success;
    }

    @Override
    public boolean offer(final E element, final long timeout, final TimeUnit unit) throws InterruptedException {
        final boolean success = super.offer(element, timeout, unit);
        if (success) {
            updateLargestSize();
        }
        return success;
    }

    @Override
    public void put(final E element) throws InterruptedException {
        super.put(element);
        updateLargestSize();
    }

    /**
     * Get the largest size recorded
     *
     * @return Current largest size
     */
    public int getLargestSize() {
        return largestSize.get();
    }

    private void updateLargestSize() {
        largestSize.getAndAccumulate(size(), Math::max);
    }
}

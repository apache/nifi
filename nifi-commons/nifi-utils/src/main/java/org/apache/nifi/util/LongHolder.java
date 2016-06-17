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
package org.apache.nifi.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @deprecated As of release 1.0.1. Please use {@link AtomicLong}
 *
 * Wraps a Long value so that it can be declared <code>final</code> and still be accessed from inner classes;
 * the functionality is similar to that of an AtomicLong, but operations on this class
 * are not atomic. This results in greater performance when the atomicity is not needed.
 */

@Deprecated
public class LongHolder extends ObjectHolder<Long> {

    public LongHolder(final long initialValue) {
        super(initialValue);
    }

    public long addAndGet(final long delta) {
        final long curValue = get();
        final long newValue = curValue + delta;
        set(newValue);
        return newValue;
    }

    public long getAndAdd(final long delta) {
        final long curValue = get();
        final long newValue = curValue + delta;
        set(newValue);
        return curValue;
    }

    public long incrementAndGet() {
        return addAndGet(1);
    }

    public long getAndIncrement() {
        return getAndAdd(1);
    }

    public long decrementAndGet() {
        return addAndGet(-1L);
    }

    public long getAndDecrement() {
        return getAndAdd(-1L);
    }
}

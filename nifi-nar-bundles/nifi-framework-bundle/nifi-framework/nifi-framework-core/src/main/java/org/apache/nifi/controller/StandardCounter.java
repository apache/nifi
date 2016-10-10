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
package org.apache.nifi.controller;

import java.util.concurrent.atomic.AtomicLong;

public class StandardCounter implements Counter {

    private final String identifier;
    private final String context;
    private final String name;
    private final AtomicLong value;

    public StandardCounter(final String identifier, final String context, final String name) {
        this.identifier = identifier;
        this.context = context;
        this.name = name;
        this.value = new AtomicLong(0L);
    }

    @Override
    public void adjust(final long delta) {
        this.value.addAndGet(delta);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getValue() {
        return this.value.get();
    }

    @Override
    public String getContext() {
        return context;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void reset() {
        this.value.set(0);
    }

    @Override
    public String toString() {
        return "Counter[identifier=" + identifier + ']';
    }

    public static UnmodifiableCounter unmodifiableCounter(final Counter counter) {
        return new UnmodifiableCounter(counter);
    }

    static class UnmodifiableCounter extends StandardCounter {

        private final Counter counter;

        public UnmodifiableCounter(final Counter counter) {
            super(counter.getIdentifier(), counter.getContext(), counter.getName());
            this.counter = counter;
        }

        @Override
        public void adjust(long delta) {
            throw new UnsupportedOperationException("Cannot modify value of UnmodifiableCounter");
        }

        @Override
        public String getName() {
            return counter.getName();
        }

        @Override
        public long getValue() {
            return counter.getValue();
        }

        @Override
        public String getContext() {
            return counter.getContext();
        }

        @Override
        public String getIdentifier() {
            return counter.getIdentifier();
        }

        @Override
        public String toString() {
            return counter.toString();
        }
    }
}

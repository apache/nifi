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
package org.apache.nifi.controller.repository;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.StandardCounter;

public class StandardCounterRepository implements CounterRepository {

    private final ConcurrentMap<String, ConcurrentMap<String, Counter>> processorCounters = new ConcurrentHashMap<>();

    @Override
    public Counter getCounter(final String counterContext, final String name) {
        return StandardCounter.unmodifiableCounter(getModifiableCounter(counterContext, name));
    }

    private String getIdentifier(final String counterContext, final String name) {
        final String contextId = UUID.nameUUIDFromBytes(counterContext.getBytes(StandardCharsets.UTF_8)).toString();
        final String nameId = UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
        final String contextAndName = contextId + "-" + nameId;
        return UUID.nameUUIDFromBytes(contextAndName.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private Counter getModifiableCounter(final String counterContext, final String name) {
        ConcurrentMap<String, Counter> counters = processorCounters.get(counterContext);
        if (counters == null) {
            counters = new ConcurrentHashMap<>();
            final ConcurrentMap<String, Counter> oldProcessorCounters = processorCounters.putIfAbsent(counterContext, counters);
            if (oldProcessorCounters != null) {
                counters = oldProcessorCounters;
            }
        }

        Counter counter = counters.get(name);
        if (counter == null) {
            counter = new StandardCounter(getIdentifier(counterContext, name), counterContext, name);
            final Counter oldCounter = counters.putIfAbsent(name, counter);
            if (oldCounter != null) {
                counter = oldCounter;
            }
        }

        return counter;
    }

    @Override
    public void adjustCounter(final String counterContext, final String name, final long delta) {
        getModifiableCounter(counterContext, name).adjust(delta);
    }

    @Override
    public List<Counter> getCounters(final String counterContext) {
        final List<Counter> counters = new ArrayList<>();
        final Map<String, Counter> map = processorCounters.get(counterContext);
        if (map == null) {
            return counters;
        }
        for (final Counter counter : map.values()) {
            counters.add(StandardCounter.unmodifiableCounter(counter));
        }
        return counters;
    }

    @Override
    public List<Counter> getCounters() {
        final List<Counter> counters = new ArrayList<>();
        for (final Map<String, Counter> map : processorCounters.values()) {
            for (final Counter counter : map.values()) {
                counters.add(StandardCounter.unmodifiableCounter(counter));
            }
        }
        return counters;
    }

    @Override
    public Counter resetCounter(final String identifier) {
        for (final ConcurrentMap<String, Counter> counters : processorCounters.values()) {
            for (final Counter counter : counters.values()) {
                if (counter.getIdentifier().equals(identifier)) {
                    counter.reset();
                    return counter;
                }
            }
        }
        return null;
    }
}

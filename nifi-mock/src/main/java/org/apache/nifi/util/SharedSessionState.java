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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceReporter;

public class SharedSessionState {

    private final MockFlowFileQueue flowFileQueue;
    private final ProvenanceReporter provenanceReporter;
    @SuppressWarnings("unused")
    private final Processor processor;
    private final AtomicLong flowFileIdGenerator;
    private final ConcurrentMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();
    private final Set<ProvenanceEventRecord> events = new LinkedHashSet<>();

    public SharedSessionState(final Processor processor, final AtomicLong flowFileIdGenerator) {
        flowFileQueue = new MockFlowFileQueue();
        provenanceReporter = new MockProvenanceReporter(null, this, UUID.randomUUID().toString(), "N/A");
        this.flowFileIdGenerator = flowFileIdGenerator;
        this.processor = processor;
    }

    void addProvenanceEvents(final Collection<ProvenanceEventRecord> events) {
        this.events.addAll(events);
    }

    void clearProvenanceEvents() {
        this.events.clear();
    }

    public List<ProvenanceEventRecord> getProvenanceEvents() {
        return new ArrayList<>(this.events);
    }

    public MockFlowFileQueue getFlowFileQueue() {
        return flowFileQueue;
    }

    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    public long nextFlowFileId() {
        return flowFileIdGenerator.getAndIncrement();
    }

    public void adjustCounter(final String name, final long delta) {
        AtomicLong counter = counterMap.get(name);
        if (counter == null) {
            counter = new AtomicLong(0L);
            final AtomicLong existingCounter = counterMap.putIfAbsent(name, counter);
            if (existingCounter != null) {
                counter = existingCounter;
            }
        }

        counter.addAndGet(delta);
    }

    public Long getCounterValue(final String name) {
        final AtomicLong counterValue = counterMap.get(name);
        return counterValue == null ? null : counterValue.get();
    }
}

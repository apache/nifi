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
package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.StandardRepositoryStatusReport;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RingBufferEventRepository implements FlowFileEventRepository {

    private final int numMinutes;
    private final ConcurrentMap<String, EventContainer> componentEventMap = new ConcurrentHashMap<>();

    public RingBufferEventRepository(final int numMinutes) {
        this.numMinutes = numMinutes;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void updateRepository(final FlowFileEvent event, final String componentId) {
        final EventContainer eventContainer = componentEventMap.computeIfAbsent(componentId, id -> new SecondPrecisionEventContainer(numMinutes));
        eventContainer.addEvent(event);
    }

    @Override
    public StandardRepositoryStatusReport reportTransferEvents(final long sinceEpochMillis) {
        final StandardRepositoryStatusReport report = new StandardRepositoryStatusReport();

        componentEventMap.forEach((componentId, container) -> report.addReportEntry(container.generateReport(sinceEpochMillis), componentId));
        return report;
    }

    @Override
    public void purgeTransferEvents(final long cutoffEpochMilliseconds) {
        // This is done so that if a processor is removed from the graph, its events
        // will be removed rather than being kept in memory
        for (final EventContainer container : componentEventMap.values()) {
            container.purgeEvents(cutoffEpochMilliseconds);
        }
    }

    @Override
    public void purgeTransferEvents(String componentIdentifier) {
        componentEventMap.remove(componentIdentifier);
    }

}

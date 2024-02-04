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
package org.apache.nifi.controller.status.history.storage.questdb;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.history.ComponentDetailsStorage;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;

import java.util.HashMap;
import java.util.Map;

public class QuestDbConnectionStatusStorage extends QuestDbComponentStatusStorage<ConnectionStatus> {
    private static final Map<Integer, MetricDescriptor<ConnectionStatus>> METRICS = new HashMap<>();

    static {
        METRICS.put(2, ConnectionStatusDescriptor.INPUT_BYTES.getDescriptor());
        METRICS.put(3, ConnectionStatusDescriptor.INPUT_COUNT.getDescriptor());
        METRICS.put(4, ConnectionStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        METRICS.put(5, ConnectionStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        METRICS.put(6, ConnectionStatusDescriptor.QUEUED_BYTES.getDescriptor());
        METRICS.put(7, ConnectionStatusDescriptor.QUEUED_COUNT.getDescriptor());
        METRICS.put(8, ConnectionStatusDescriptor.TOTAL_QUEUED_DURATION.getDescriptor());
        METRICS.put(9, ConnectionStatusDescriptor.MAX_QUEUED_DURATION.getDescriptor());
        METRICS.put(10, ConnectionStatusDescriptor.AVERAGE_QUEUED_DURATION.getDescriptor());
    }

    public QuestDbConnectionStatusStorage(final QuestDbContext dbContext, final ComponentDetailsStorage componentDetails) {
        super(dbContext, componentDetails);
    }

    @Override
    protected String extractId(final ConnectionStatus statusEntry) {
        return statusEntry.getId();
    }

    @Override
    protected Map<Integer, MetricDescriptor<ConnectionStatus>> getMetrics() {
        return METRICS;
    }

    @Override
    protected String getTableName() {
        return "connectionStatus";
    }
}

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

import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.ComponentDetailsStorage;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.RemoteProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;

import java.util.HashMap;
import java.util.Map;

public class QuestDbRemoteProcessGroupStatusStorage extends QuestDbComponentStatusStorage<RemoteProcessGroupStatus> {
    private static final Map<Integer, MetricDescriptor<RemoteProcessGroupStatus>> METRICS = new HashMap<>();

    static {
        METRICS.put(2, RemoteProcessGroupStatusDescriptor.SENT_BYTES.getDescriptor());
        METRICS.put(3, RemoteProcessGroupStatusDescriptor.SENT_COUNT.getDescriptor());
        METRICS.put(4, RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES.getDescriptor());
        METRICS.put(5, RemoteProcessGroupStatusDescriptor.RECEIVED_COUNT.getDescriptor());
        METRICS.put(6, RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES_PER_SECOND.getDescriptor());
        METRICS.put(7, RemoteProcessGroupStatusDescriptor.SENT_BYTES_PER_SECOND.getDescriptor());
        METRICS.put(8, RemoteProcessGroupStatusDescriptor.TOTAL_BYTES_PER_SECOND.getDescriptor());
        METRICS.put(9, RemoteProcessGroupStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor());
    }

    public QuestDbRemoteProcessGroupStatusStorage(final QuestDbContext context, final ComponentDetailsStorage componentDetailsStorage) {
        super(context, componentDetailsStorage);
    }

    @Override
    protected String extractId(final RemoteProcessGroupStatus statusEntry) {
        return statusEntry.getId();
    }

    @Override
    protected Map<Integer, MetricDescriptor<RemoteProcessGroupStatus>> getMetrics() {
        return METRICS;
    }

    @Override
    protected String getTableName() {
        return "remoteProcessGroupStatus";
    }
}

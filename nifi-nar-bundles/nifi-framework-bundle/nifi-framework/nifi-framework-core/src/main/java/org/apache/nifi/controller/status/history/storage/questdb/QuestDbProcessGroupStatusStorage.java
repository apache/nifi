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

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.history.ComponentDetailsStorage;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.ProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;

import java.util.HashMap;
import java.util.Map;

public class QuestDbProcessGroupStatusStorage extends QuestDbComponentStatusStorage<ProcessGroupStatus> {
    private static final Map<Integer, MetricDescriptor<ProcessGroupStatus>> METRICS = new HashMap<>();

    static {
        METRICS.put(2, ProcessGroupStatusDescriptor.BYTES_READ.getDescriptor());
        METRICS.put(3, ProcessGroupStatusDescriptor.BYTES_WRITTEN.getDescriptor());
        METRICS.put(4, ProcessGroupStatusDescriptor.BYTES_TRANSFERRED.getDescriptor());
        METRICS.put(5, ProcessGroupStatusDescriptor.INPUT_BYTES.getDescriptor());
        METRICS.put(6, ProcessGroupStatusDescriptor.INPUT_COUNT.getDescriptor());
        METRICS.put(7, ProcessGroupStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        METRICS.put(8, ProcessGroupStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        METRICS.put(9, ProcessGroupStatusDescriptor.QUEUED_BYTES.getDescriptor());
        METRICS.put(10, ProcessGroupStatusDescriptor.QUEUED_COUNT.getDescriptor());
        METRICS.put(11, ProcessGroupStatusDescriptor.TASK_MILLIS.getDescriptor());
    }

    public QuestDbProcessGroupStatusStorage(final QuestDbContext dbContext, final ComponentDetailsStorage componentDetailsStorage) {
        super(dbContext, componentDetailsStorage);
    }

    @Override
    protected String extractId(final ProcessGroupStatus statusEntry) {
        return statusEntry.getId();
    }

    @Override
    protected Map<Integer, MetricDescriptor<ProcessGroupStatus>> getMetrics() {
        return METRICS;
    }

    @Override
    protected String getTableName() {
        return "processGroupStatus";
    }
}

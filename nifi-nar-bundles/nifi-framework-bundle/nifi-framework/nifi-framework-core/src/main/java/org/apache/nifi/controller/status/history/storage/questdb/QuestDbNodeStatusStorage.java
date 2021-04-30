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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlExecutionContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.NodeStatusDescriptor;
import org.apache.nifi.controller.status.history.StandardMetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusHistory;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityWritingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbWritingTemplate;
import org.apache.nifi.controller.status.history.storage.NodeStatusStorage;
import org.apache.nifi.controller.status.history.storage.StatusStorage;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class QuestDbNodeStatusStorage implements NodeStatusStorage {
    private static final String TABLE_NAME = "nodeStatus";

    private static final String READING_QUERY =
        "SELECT * FROM nodeStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
        "ORDER BY capturedAt ASC";

    private static final Map<Integer, MetricDescriptor<NodeStatus>> METRICS = new HashMap<>();

    private final QuestDbContext dbContext;

    static {
        METRICS.put(1, NodeStatusDescriptor.FREE_HEAP.getDescriptor());
        METRICS.put(2, NodeStatusDescriptor.USED_HEAP.getDescriptor());
        METRICS.put(3, NodeStatusDescriptor.HEAP_UTILIZATION.getDescriptor());
        METRICS.put(4, NodeStatusDescriptor.FREE_NON_HEAP.getDescriptor());
        METRICS.put(5, NodeStatusDescriptor.USED_NON_HEAP.getDescriptor());
        METRICS.put(6, NodeStatusDescriptor.OPEN_FILE_HANDLES.getDescriptor());
        METRICS.put(7, NodeStatusDescriptor.PROCESSOR_LOAD_AVERAGE.getDescriptor());
        METRICS.put(8, NodeStatusDescriptor.TOTAL_THREADS.getDescriptor());
        METRICS.put(9, NodeStatusDescriptor.EVENT_DRIVEN_THREADS.getDescriptor());
        METRICS.put(10, NodeStatusDescriptor.TIME_DRIVEN_THREADS.getDescriptor());
    }

    private static final QuestDbEntityWritingTemplate<NodeStatus> WRITING_TEMPLATE
        = new QuestDbEntityWritingTemplate<>(TABLE_NAME, (statusEntry, row) -> METRICS.keySet().forEach(ord -> row.putLong(ord, METRICS.get(ord).getValueFunction().getValue(statusEntry))));

    private static final StorageStatusReadingTemplate STORAGE_READING_TEMPLATE = new StorageStatusReadingTemplate();

    private static final QuestDbWritingTemplate<Pair<Instant, NodeStatus>> STORAGE_WRITING_TEMPLATE = new StorageStatusWritingTemplate();

    public QuestDbNodeStatusStorage(final QuestDbContext dbContext) {
        this.dbContext = dbContext;
    }

    @Override
    public StatusHistory read(final Instant start, final Instant end) {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> storageMetrics = STORAGE_READING_TEMPLATE
                .read(dbContext.getEngine(), executionContext, Arrays.asList(DATE_FORMATTER.format(start), DATE_FORMATTER.format(end)));
        final NodeStatusReadingTemplate nodeStatusReadingTemplate = new NodeStatusReadingTemplate(storageMetrics);
        final List<StatusSnapshot> snapshots = nodeStatusReadingTemplate
                .read(dbContext.getEngine(), executionContext, Arrays.asList(DATE_FORMATTER.format(start), DATE_FORMATTER.format(end)));
        return new StandardStatusHistory(snapshots, new HashMap<>(), new Date());
    }

    @Override
    public void store(final List<Pair<Instant, NodeStatus>> statusEntries) {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        WRITING_TEMPLATE.insert(dbContext.getEngine(), executionContext, statusEntries);
        STORAGE_WRITING_TEMPLATE.insert(dbContext.getEngine(), executionContext, statusEntries);
    }

    public static Map<Integer, MetricDescriptor<NodeStatus>> getMetrics() {
        return METRICS;
    }

    private static class NodeStatusReadingTemplate extends QuestDbReadingTemplate<List<StatusSnapshot>> {
        private final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> storageMetricsByTime;

        public NodeStatusReadingTemplate(final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> storageMetricsByTime) {
            super(READING_QUERY, e -> Collections.emptyList());
            this.storageMetricsByTime = storageMetricsByTime;
        }

        @Override
        protected List<StatusSnapshot> processResult(final RecordCursor cursor) {
            final List<StatusSnapshot> entities = new LinkedList<>();

            while (cursor.hasNext()) {
                entities.add(map(cursor.getRecord()));
            }

            return entities;
        }

        private StandardStatusSnapshot map(final Record record) {
            final long createdAt = TimeUnit.MICROSECONDS.toMillis(record.getTimestamp(0));
            final Map<StandardMetricDescriptor<NodeStatus>, Long> statusMetrics = storageMetricsByTime.get(createdAt);
            final Set<MetricDescriptor<?>> snapshotMetrics = new HashSet<>(METRICS.values().size() + statusMetrics.keySet().size());
            snapshotMetrics.addAll(METRICS.values());
            snapshotMetrics.addAll(statusMetrics.keySet());
            final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(snapshotMetrics);
            snapshot.setTimestamp(new Date(createdAt));
            METRICS.keySet().forEach(ordinal -> snapshot.addStatusMetric(METRICS.get(ordinal), record.getLong(ordinal)));
            statusMetrics.entrySet().forEach(entry -> snapshot.addStatusMetric(entry.getKey(), entry.getValue()));
            return snapshot;
        }
    }
}

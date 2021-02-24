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
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.ComponentDetailsStorage;
import org.apache.nifi.controller.status.history.CounterMetricDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.ProcessorStatusDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusHistory;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityWritingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbStatusSnapshotMapper;
import org.apache.nifi.controller.status.history.questdb.QuestDbWritingTemplate;
import org.apache.nifi.controller.status.history.storage.ProcessorStatusStorage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QuestDbProcessorStatusStorage implements ProcessorStatusStorage {
    private static final String TABLE_NAME = "processorStatus";
    private static final Map<Integer, MetricDescriptor<ProcessorStatus>> METRICS = new HashMap<>();

    static {
        METRICS.put(2, ProcessorStatusDescriptor.BYTES_READ.getDescriptor());
        METRICS.put(3, ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor());
        METRICS.put(4, ProcessorStatusDescriptor.BYTES_TRANSFERRED.getDescriptor());
        METRICS.put(5, ProcessorStatusDescriptor.INPUT_BYTES.getDescriptor());
        METRICS.put(6, ProcessorStatusDescriptor.INPUT_COUNT.getDescriptor());
        METRICS.put(7, ProcessorStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        METRICS.put(8, ProcessorStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        METRICS.put(9, ProcessorStatusDescriptor.TASK_COUNT.getDescriptor());
        METRICS.put(10, ProcessorStatusDescriptor.TASK_MILLIS.getDescriptor());
        METRICS.put(11, ProcessorStatusDescriptor.TASK_NANOS.getDescriptor());
        METRICS.put(12, ProcessorStatusDescriptor.FLOWFILES_REMOVED.getDescriptor());
        METRICS.put(13, ProcessorStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor());
        METRICS.put(14, ProcessorStatusDescriptor.AVERAGE_TASK_NANOS.getDescriptor());
    }

    /**
     * In case of component status entries the first two columns are fixed (measurement time and component id) and all
     * the following fields are metric values using long format. The list of these are specified by the implementation class.
     */
    private final QuestDbEntityWritingTemplate<ProcessorStatus> writingTemplate =  new QuestDbEntityWritingTemplate<>(
            TABLE_NAME,
            (statusEntry, row) -> {
                row.putSym(1, statusEntry.getId());
                METRICS.keySet().forEach(ordinal -> row.putLong(ordinal, METRICS.get(ordinal).getValueFunction().getValue(statusEntry)));
            });

    private static final QuestDbWritingTemplate<Pair<Instant, ProcessorStatus>> counterWritingTemplate = new ComponentCounterWritingTemplate();

    private final Function<Record, StandardStatusSnapshot> statusSnapshotMapper = new QuestDbStatusSnapshotMapper(METRICS);

    private final QuestDbEntityReadingTemplate<StandardStatusSnapshot, List<StandardStatusSnapshot>> readingTemplate
            = new QuestDbEntityReadingTemplate<>(QUERY_TEMPLATE, statusSnapshotMapper, e -> e, e -> Collections.emptyList());

    private final QuestDbContext dbContext;
    private final ComponentDetailsStorage componentDetailsStorage;

    public QuestDbProcessorStatusStorage(final QuestDbContext dbContext, final ComponentDetailsStorage componentDetailsStorage) {
        this.dbContext = dbContext;
        this.componentDetailsStorage = componentDetailsStorage;
    }

    @Override
    public StatusHistory read(final String componentId, final Instant start, final Instant end, final int preferredDataPoints) {
        final List<StandardStatusSnapshot> snapshots = readingTemplate.read(
                dbContext.getEngine(),
                dbContext.getSqlExecutionContext(),
                Arrays.asList(TABLE_NAME, componentId, DATE_FORMATTER.format(start), DATE_FORMATTER.format(end)));
        return new StandardStatusHistory(
                new ArrayList<>(snapshots.subList(Math.max(snapshots.size() - preferredDataPoints, 0), snapshots.size())),
                componentDetailsStorage.getDetails(componentId),
                new Date()
        );
    }

    @Override
    public StatusHistory readWithCounter(final String componentId, final Instant start, final Instant end, final int preferredDataPoints) {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        final List<StandardStatusSnapshot> snapshots = readingTemplate.read(
                dbContext.getEngine(),
                executionContext,
                Arrays.asList(TABLE_NAME, componentId, DATE_FORMATTER.format(start), DATE_FORMATTER.format(end)));
        final CounterReadingTemplate counterReadingTemplate = new CounterReadingTemplate(snapshots);
        final List<StatusSnapshot> enrichedSnapshots = new ArrayList<>(counterReadingTemplate.read(
                dbContext.getEngine(),
                executionContext,
                Arrays.asList("componentCounter", componentId, DATE_FORMATTER.format(start), DATE_FORMATTER.format(end))));
        return new StandardStatusHistory(
                enrichedSnapshots.subList(Math.max(snapshots.size() - preferredDataPoints, 0), snapshots.size()),
                componentDetailsStorage.getDetails(componentId),
                new Date()
        );
    }

    @Override
    public void store(final List<Pair<Instant, ProcessorStatus>> statusEntries) {
        final SqlExecutionContext executionContext = dbContext.getSqlExecutionContext();
        writingTemplate.insert(dbContext.getEngine(), executionContext, statusEntries);
        counterWritingTemplate.insert(dbContext.getEngine(), executionContext, statusEntries);
    }

    private final class CounterReadingTemplate extends QuestDbReadingTemplate<List<StandardStatusSnapshot>> {
        private final List<StandardStatusSnapshot> processorStatusSnapshots;

        public CounterReadingTemplate(final List<StandardStatusSnapshot> processorStatusSnapshots) {
            super(QUERY_TEMPLATE, e -> Collections.emptyList());
            this.processorStatusSnapshots = processorStatusSnapshots;
        }

        @Override
        protected List<StandardStatusSnapshot> processResult(final RecordCursor cursor) {
            final Map<Long, StandardStatusSnapshot> snapshotsByTime = processorStatusSnapshots.stream().collect(Collectors.toMap(s -> s.getTimestamp().getTime(), s -> s));

            while (cursor.hasNext()) {
                final Record record = cursor.getRecord();
                final long recordCreatedAt = TimeUnit.MICROSECONDS.toMillis(record.getTimestamp(0));
                final StandardStatusSnapshot snapshot = snapshotsByTime.get(recordCreatedAt);
                final String counterName = new StringBuilder(record.getSym(2)).toString();
                final long counterValue = record.getLong(3);
                final MetricDescriptor<ProcessorStatus> metricDescriptor = new CounterMetricDescriptor<>(
                        counterName,
                        counterName + " (5 mins)",
                        counterName + " (5 mins)",
                        MetricDescriptor.Formatter.COUNT,
                        s -> s.getCounters() == null ? null : s.getCounters().get(counterName));
                snapshot.addStatusMetric(metricDescriptor, counterValue);
            }

            return processorStatusSnapshots;
        }
    }
}

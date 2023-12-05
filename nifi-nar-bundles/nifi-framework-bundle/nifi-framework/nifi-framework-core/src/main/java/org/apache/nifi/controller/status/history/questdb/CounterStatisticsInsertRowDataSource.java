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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.questdb.InsertRowContext;
import org.apache.nifi.questdb.InsertRowDataSource;

import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

final class CounterStatisticsInsertRowDataSource implements InsertRowDataSource {
    private final Iterator<CounterStatistic> counterStatistics;

    public CounterStatisticsInsertRowDataSource(final Iterator<CounterStatistic> counterStatistics) {
        this.counterStatistics = counterStatistics;
    }

    @Override
    public boolean hasNextToInsert() {
        return counterStatistics.hasNext();
    }

    @Override
    public void fillRowData(final InsertRowContext context) {
        final CounterStatistic counterStatistic = counterStatistics.next();

        context.initializeRow(Instant.ofEpochMilli(counterStatistic.getCapturedAt()));
        context.addString(1, counterStatistic.getComponentId());
        context.addString(2, counterStatistic.getName());
        context.addLong(3, counterStatistic.getValue());
    }

    static InsertRowDataSource getInstance(Collection<ProcessorStatus> processorStatuses)  {
        final List<CounterStatistic> counterStatistics = new LinkedList<>();

        for (final ProcessorStatus processorStatus : processorStatuses) {
            if (processorStatus.getCounters() != null) {
                for (final Map.Entry<String, Long> counter : processorStatus.getCounters().entrySet()) {
                    counterStatistics.add(new CounterStatistic(
                        processorStatus.getCreatedAtInMs(),
                        processorStatus.getId(),
                        counter.getKey(),
                        counter.getValue()
                    ));
                }
            }
        }

        return new CounterStatisticsInsertRowDataSource(counterStatistics.iterator());
    }

    private static class CounterStatistic {
        private final long capturedAt;
        private final String componentId;
        private final String name;
        private final long value;


        CounterStatistic(final long capturedAt, final String componentId, final String name, final long value) {
            this.capturedAt = capturedAt;
            this.componentId = componentId;
            this.name = name;
            this.value = value;
        }

        public long getCapturedAt() {
            return capturedAt;
        }

        public String getComponentId() {
            return componentId;
        }

        public String getName() {
            return name;
        }

        public long getValue() {
            return value;
        }
    }
}

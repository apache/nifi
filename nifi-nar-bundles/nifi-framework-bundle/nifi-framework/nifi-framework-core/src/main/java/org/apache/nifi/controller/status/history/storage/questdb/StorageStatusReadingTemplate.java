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
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StandardMetricDescriptor;
import org.apache.nifi.controller.status.history.questdb.QuestDbReadingTemplate;
import org.apache.nifi.controller.status.history.storage.StatusStorage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StorageStatusReadingTemplate extends QuestDbReadingTemplate<Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>>> {
    private static final String STORAGE_FREE_DESCRIPTION = "The usable space available for use by the underlying storage mechanism.";
    private static final String STORAGE_USED_DESCRIPTION = "The space in use on the underlying storage mechanism";

    private static final String STORAGE_READING_QUERY =
            "SELECT * FROM storageStatus " +
                    "WHERE capturedAt > to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
                    "AND capturedAt < to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
                    "ORDER BY capturedAt ASC";

    public StorageStatusReadingTemplate() {
        super(STORAGE_READING_QUERY, e -> Collections.emptyMap());
    }

    @Override
    protected Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> processResult(final RecordCursor cursor) {
        final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> result = new HashMap<>();

        int storageNumber = 1;

        while (cursor.hasNext()) {
            final Record record = cursor.getRecord();
            final long createdAt = TimeUnit.MICROSECONDS.toMillis(record.getTimestamp(0));
            final short type = record.getShort(2);
            final CharSequence name = record.getSym(1);

            if (!result.containsKey(createdAt)) {
                result.put(createdAt, new HashMap<>());
            }

            result.get(createdAt).put(getDescriptor(
                    QuestDbNodeStatusStorage.getMetrics().size() + result.get(createdAt).size(),
                    getField(type, storageNumber, StorageMetric.FREE),
                    getLabel(type, name, StorageMetric.FREE),
                    STORAGE_FREE_DESCRIPTION
            ), record.getLong(3));
            result.get(createdAt).put(getDescriptor(
                    QuestDbNodeStatusStorage.getMetrics().size() + result.get(createdAt).size(),
                    getField(type, storageNumber, StorageMetric.USED),
                    getLabel(type, name, StorageMetric.USED),
                    STORAGE_USED_DESCRIPTION
            ), record.getLong(4));
            storageNumber++;
        }

        return result;
    }

    private StandardMetricDescriptor<NodeStatus> getDescriptor(final int ordinal, final String field, final String label, final String description) {
        return new StandardMetricDescriptor<>(() -> ordinal, field, label, STORAGE_FREE_DESCRIPTION, MetricDescriptor.Formatter.DATA_SIZE, v -> 0L);
    }

    private String getField(final int type, final int storageNumber, final StorageMetric storageMetric) {
        return new StringBuilder(StorageType.getById(type).getField()).append(storageNumber).append(storageMetric.getField()).toString();
    }

    private String getLabel(final int type, final CharSequence name, final StorageMetric storageMetric) {
        return new StringBuilder(StorageType.getById(type).getLabel()).append(" (").append(name).append(") ").append(storageMetric.getLabel()).toString();
    }

    private enum StorageType {
        CONTENT("contentStorage", "Content Repository"), // 0
        PROVENANCE("provenanceStorage", "Provenance Repository"); // 1

        private final String field;
        private final String label;

        StorageType(final String field, final String label) {
            this.field = field;
            this.label = label;
        }

        public static StorageType getById(final int id) {
            return StorageType.values()[id];
        }

        public String getField() {
            return field;
        }

        public String getLabel() {
            return label;
        }
    }

    private enum StorageMetric {
        FREE("Free", "Free Space"), USED("Used", "Used Space");

        private final String field;
        private final String label;

        StorageMetric(final String field, final String label) {
            this.field =  field;
            this.label = label;
        }

        public String getField() {
            return field;
        }

        public String getLabel() {
            return label;
        }
    }
}

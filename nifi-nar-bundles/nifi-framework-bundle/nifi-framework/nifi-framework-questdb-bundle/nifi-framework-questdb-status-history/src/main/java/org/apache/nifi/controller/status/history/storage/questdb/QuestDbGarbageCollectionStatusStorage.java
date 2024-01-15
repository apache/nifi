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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.controller.status.history.GarbageCollectionHistory;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionHistory;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityReadingTemplate;
import org.apache.nifi.controller.status.history.questdb.QuestDbEntityWritingTemplate;
import org.apache.nifi.controller.status.history.storage.GarbageCollectionStatusStorage;
import org.apache.nifi.controller.status.history.storage.StatusStorage;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class QuestDbGarbageCollectionStatusStorage implements GarbageCollectionStatusStorage {
    private static final String TABLE_NAME = "garbageCollectionStatus";

    private static final String QUERY =
        "SELECT * FROM garbageCollectionStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT +  "') " +
        "ORDER BY capturedAt ASC";

    private static final QuestDbEntityWritingTemplate<GarbageCollectionStatus> WRITING_TEMPLATE =  new QuestDbEntityWritingTemplate<>(
        TABLE_NAME,
        (statusEntry, row) -> {
            row.putSym(1, statusEntry.getMemoryManagerName());
            row.putLong(2, statusEntry.getCollectionCount());
            row.putLong(3, statusEntry.getCollectionMillis());
        });


    private static final QuestDbEntityReadingTemplate<GarbageCollectionStatus, GarbageCollectionHistory> READING_TEMPLATE = new QuestDbEntityReadingTemplate<>(
        QUERY,
        record ->
            new StandardGarbageCollectionStatus(new StringBuilder(record.getSym(1)).toString(), new Date(record.getTimestamp(0)), record.getLong(2), record.getLong(3)),
        garbageCollectionStatuses -> {
            final StandardGarbageCollectionHistory result = new StandardGarbageCollectionHistory();
            garbageCollectionStatuses.forEach(status -> result.addGarbageCollectionStatus(status));
            return result;
        },
        e -> new StandardGarbageCollectionHistory()
    );

    private final QuestDbContext context;

    public QuestDbGarbageCollectionStatusStorage(final QuestDbContext context) {
        this.context = context;
    }

    @Override
    public GarbageCollectionHistory read(final Instant start, final Instant end) {
        return READING_TEMPLATE.read(context.getEngine(), context.getSqlExecutionContext(), Arrays.asList(DATE_FORMATTER.format(start), DATE_FORMATTER.format(end)));
    }

    @Override
    public void store(final List<Pair<Instant, GarbageCollectionStatus>> statusEntries) {
        WRITING_TEMPLATE.insert(context.getEngine(), context.getSqlExecutionContext(), statusEntries);
    }
}
